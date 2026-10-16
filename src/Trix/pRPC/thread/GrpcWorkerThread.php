<?php

declare(strict_types=1);

namespace Trix\pRPC\thread;

use Grpc\ChannelCredentials;
use pmmp\thread\ThreadSafeArray;
use pocketmine\snooze\SleeperHandlerEntry;
use pocketmine\snooze\SleeperNotifier;
use pocketmine\thread\Thread;
use RuntimeException;
use Throwable;
use Trix\pRPC\internal\RawGrpcStub;
use Trix\pRPC\internal\RawMessage;
use Trix\pRPC\retry\RpcStatus;

/** @internal */
final class GrpcWorkerThread extends Thread{
    private const int MAX_ERROR_MESSAGE_BYTES = 8_192;

    public const int STATE_STARTING = 0;
    public const int STATE_READY = 1;
    public const int STATE_BACKOFF = 2;
    public const int STATE_STOPPING = 3;
    public const int STATE_FAILED = 4;
    public const int STATE_STUCK = 5;

    public const int CONNECTIVITY_UNKNOWN = -1;
    public const int CONNECTIVITY_IDLE = 0;
    public const int CONNECTIVITY_CONNECTING = 1;
    public const int CONNECTIVITY_READY = 2;
    public const int CONNECTIVITY_TRANSIENT_FAILURE = 3;
    public const int CONNECTIVITY_SHUTDOWN = 4;

    public const string PROBE_JOB = 'probe';
    /** pack('JJJNN', id, deadlineNs, queuedNs, methodId, metadataLength) then metadata then the request */
    public const int JOB_HEADER_BYTES = 32;
    /** pack('NJJN', endpointId, id, queueWaitUs, statusCode) then the response, or serialize([message, metadata]) */
    public const int RESULT_HEADER_BYTES = 24;

    private int $state = self::STATE_STARTING;
    private bool $stoppedNormally = false;
    private string $lastError = '';
    private int $busyEndpoint = -1;
    private int $busyUntilNs = 0;
    private string $inflight = '';

    public function __construct(
        private readonly RpcDispatchQueue $queue,
        private readonly SleeperHandlerEntry $sleeperEntry,
        public readonly int $workerId
    ){}

    /** Called by the queue while it holds its lock. */
    public function markBusy(int $endpointId, int $untilNs, string $ids) : void{
        $this->synchronized(function() use ($endpointId, $untilNs, $ids) : void{
            $this->busyEndpoint = $endpointId;
            $this->busyUntilNs = $untilNs;
            $this->inflight = $ids;
        });
    }

    public function markIdle() : void{
        $this->synchronized(function() : void{
            $this->busyEndpoint = -1;
            $this->busyUntilNs = 0;
            $this->inflight = '';
        });
    }

    public function busyEndpoint() : int{
        return $this->busyEndpoint;
    }

    /** @return array{endpoint: int, untilNs: int, ids: list<int>, state: int} */
    public function snapshot() : array{
        return $this->synchronized(fn() : array => [
            'endpoint' => $this->busyEndpoint,
            'untilNs' => $this->busyUntilNs,
            'ids' => $this->inflight === '' ? [] : array_values(unpack('J*', $this->inflight)),
            'state' => $this->state,
        ]);
    }

    public function getLastError() : string{
        return $this->synchronized(fn() : string => $this->lastError);
    }

    public function stoppedNormally() : bool{
        return $this->synchronized(fn() : bool => $this->stoppedNormally);
    }

    public function quit() : void{
        $this->queue->stop();
        parent::quit();
    }

    protected function onRun() : void{
        $notifier = $this->sleeperEntry->createNotifier();
        $results = $this->queue->results;
        /** @var array<int, array<string, mixed>|null> $configs */
        $configs = [];
        /** @var array<int, RawGrpcStub> $stubs */
        $stubs = [];
        /** @var array<int, array{untilNs: int, delayMs: int}> $backoff */
        $backoff = [];
        $epoch = -1;
        $normalExit = false;
        $this->setState(self::STATE_READY);

        try{
            while(($batch = $this->queue->take($this)) !== null){
                [$endpointId, $jobs] = $batch;

                $currentEpoch = $this->queue->epoch();
                if($currentEpoch !== $epoch){
                    $epoch = $currentEpoch;
                    foreach($configs as $id => $_){
                        if($this->queue->laneConfig($id) === null){
                            if(isset($stubs[$id])){
                                self::closeStub($stubs[$id]);
                            }
                            unset($configs[$id], $stubs[$id], $backoff[$id]);
                        }
                    }
                }
                if(!array_key_exists($endpointId, $configs)){
                    $raw = $this->queue->laneConfig($endpointId);
                    $configs[$endpointId] = $raw === null ? null : unserialize($raw, ['allowed_classes' => false]);
                }

                [$connectivity, $probed] = $this->runBatch($endpointId, $configs[$endpointId], $jobs, $stubs, $backoff, $results, $notifier);
                if(!$this->queue->finish($this, $endpointId, $connectivity, $probed)){
                    break;
                }
            }
            $normalExit = true;
        }finally{
            foreach($stubs as $stub){
                self::closeStub($stub);
            }
            $this->synchronized(function() use ($normalExit) : void{
                $this->stoppedNormally = $normalExit;
                $this->state = $normalExit ? self::STATE_STOPPING : self::STATE_FAILED;
            });
        }
    }

    /**
     * @param array<string, mixed>|null $config
     * @param list<string> $jobs
     * @param array<int, RawGrpcStub> $stubs
     * @param array<int, array{untilNs: int, delayMs: int}> $backoff
     * @param ThreadSafeArray<int, string> $results
     * @return array{int, bool} connectivity after the batch and whether it carried a probe
     */
    private function runBatch(int $endpointId, ?array $config, array $jobs, array &$stubs, array &$backoff, ThreadSafeArray $results, SleeperNotifier $notifier) : array{
        $probed = false;
        $parsed = [];
        foreach($jobs as $job){
            if($job === self::PROBE_JOB){
                $probed = true;
                continue;
            }
            if(strlen($job) < self::JOB_HEADER_BYTES){
                continue;
            }
            $h = unpack('Jid/Jdeadline/Jqueued/Nmethod/Nmeta', $job);
            $metadata = [];
            if($h['meta'] > 0){
                $metadata = unserialize(substr($job, self::JOB_HEADER_BYTES, $h['meta']), ['allowed_classes' => false]);
                if(!is_array($metadata)){
                    $metadata = [];
                }
            }
            $parsed[] = [$h['id'], $h['deadline'], $h['queued'], $h['method'], $metadata, substr($job, self::JOB_HEADER_BYTES + $h['meta'])];
        }

        if($config === null){
            foreach($parsed as [$id, , $queued]){
                $this->publishError($results, $notifier, $endpointId, $id, $queued, RpcStatus::UNAVAILABLE, 'RPC endpoint was removed.', [], 0);
            }
            return [self::CONNECTIVITY_UNKNOWN, $probed];
        }

        $now = hrtime(true);
        $stub = $stubs[$endpointId] ?? null;
        if($stub === null){
            $wait = $backoff[$endpointId] ?? null;
            $error = null;
            if($wait !== null && $wait['untilNs'] > $now){
                $error = 'gRPC channel is backing off after a failed setup: ' . $this->getLastError();
            }else{
                try{
                    $stub = $stubs[$endpointId] = self::createStub($config);
                    unset($backoff[$endpointId]);
                    $this->setState(self::STATE_READY, '');
                }catch(Throwable $e){
                    $delay = $wait === null ? (int) $config['reconnectBackoffMinMs'] : min($wait['delayMs'] * 2, (int) $config['reconnectBackoffMaxMs']);
                    $backoff[$endpointId] = ['untilNs' => $now + (int) ($delay * (0.8 + mt_rand() / mt_getrandmax() * 0.4)) * 1_000_000, 'delayMs' => $delay];
                    $this->setState(self::STATE_BACKOFF, $e->getMessage());
                    $error = 'Unable to initialize gRPC client: ' . $e->getMessage();
                }
            }
            if($stub === null){
                foreach($parsed as [$id, , $queued]){
                    $this->publishError($results, $notifier, $endpointId, $id, $queued, RpcStatus::UNAVAILABLE, (string) $error, [], (int) $config['maxMetadataBytes']);
                }
                return [self::CONNECTIVITY_TRANSIENT_FAILURE, $probed];
            }
        }

        if($probed){
            try{
                $stub->getConnectivityState(true);
            }catch(Throwable){
            }
        }

        /** @var list<string> $methodPaths */
        $methodPaths = $config['methodPaths'];
        $maxResponseBytes = (int) $config['maxResponseBytes'];
        $maxMetadataBytes = (int) $config['maxMetadataBytes'];
        $calls = [];
        foreach($parsed as [$id, $deadlineNs, $queuedNs, $methodId, $metadata, $requestData]){
            $now = hrtime(true);
            if($deadlineNs <= $now){
                $this->publishError($results, $notifier, $endpointId, $id, $queuedNs, RpcStatus::DEADLINE_EXCEEDED, 'RPC deadline exceeded before execution.', [], 0);
                continue;
            }
            $path = $methodPaths[$methodId] ?? null;
            if(!is_string($path)){
                $this->publishError($results, $notifier, $endpointId, $id, $queuedNs, RpcStatus::INTERNAL, 'Unknown RPC method ID.', [], 0);
                continue;
            }
            try{
                $call = $stub->rawUnary($path, $requestData, $metadata, ['timeout' => max(1, intdiv($deadlineNs - $now, 1_000))]);
                $calls[] = [$id, $deadlineNs, $queuedNs, $now, $call];
            }catch(Throwable $e){
                $this->publishError($results, $notifier, $endpointId, $id, $queuedNs, RpcStatus::INTERNAL, 'RPC dispatch failed: ' . $e->getMessage(), [], 0);
            }
        }

        // wait() blocks per call, so read the ones that expire first first
        usort($calls, static fn(array $a, array $b) : int => $a[1] <=> $b[1]);
        foreach($calls as [$id, , $queuedNs, $startedNs, $call]){
            try{
                [$response, $status] = $call->wait();
                $code = is_int($status->code ?? null) ? $status->code : RpcStatus::UNKNOWN;
                $metadata = is_array($status->metadata ?? null) ? $status->metadata : [];
                if($code !== RpcStatus::OK){
                    $details = is_string($status->details ?? null) ? $status->details : 'Unknown gRPC error.';
                    $this->publishError($results, $notifier, $endpointId, $id, $queuedNs, $code, $details, $metadata, $maxMetadataBytes, $startedNs);
                }elseif(!$response instanceof RawMessage){
                    $this->publishError($results, $notifier, $endpointId, $id, $queuedNs, RpcStatus::INTERNAL, 'gRPC returned OK without a raw protobuf response.', $metadata, $maxMetadataBytes, $startedNs);
                }elseif(strlen($response->bytes()) > $maxResponseBytes){
                    $this->publishError($results, $notifier, $endpointId, $id, $queuedNs, RpcStatus::INTERNAL, 'RPC response exceeded the configured worker limit.', $metadata, $maxMetadataBytes, $startedNs);
                }else{
                    $this->publish($results, $notifier, pack('NJJN', $endpointId, $id, intdiv($startedNs - $queuedNs, 1_000), RpcStatus::OK) . $response->bytes());
                }
            }catch(Throwable $e){
                if(isset($stubs[$endpointId])){
                    self::closeStub($stubs[$endpointId]);
                    unset($stubs[$endpointId]);
                }
                $this->publishError($results, $notifier, $endpointId, $id, $queuedNs, RpcStatus::UNAVAILABLE, 'gRPC wait failed: ' . $e->getMessage(), [], 0, $startedNs);
            }
        }

        try{
            return [isset($stubs[$endpointId]) ? $stubs[$endpointId]->getConnectivityState(false) : self::CONNECTIVITY_TRANSIENT_FAILURE, $probed];
        }catch(Throwable){
            return [self::CONNECTIVITY_UNKNOWN, $probed];
        }
    }

    /** @param array<string, mixed> $config */
    private static function createStub(array $config) : RawGrpcStub{
        $endpoint = $config['endpoint'];
        $channelOptions = $config['channelOptions'];
        $credentials = $config['credentials'];
        if(!is_string($endpoint) || !is_array($channelOptions) || !is_array($credentials)){
            throw new RuntimeException('Invalid gRPC worker configuration.');
        }

        $channelOptions['credentials'] = match($credentials['mode'] ?? null){
            'insecure' => ChannelCredentials::createInsecure(),
            'tls' => ChannelCredentials::createSsl(
                is_string($credentials['rootCertificates'] ?? null) ? $credentials['rootCertificates'] : null,
                is_string($credentials['privateKey'] ?? null) ? $credentials['privateKey'] : null,
                is_string($credentials['certificateChain'] ?? null) ? $credentials['certificateChain'] : null
            ),
            default => throw new RuntimeException('Unknown gRPC credential mode.'),
        };

        return new RawGrpcStub($endpoint, $channelOptions);
    }

    private static function closeStub(RawGrpcStub $stub) : void{
        try{
            $stub->close();
        }catch(Throwable){
        }
    }

    /**
     * @param ThreadSafeArray<int, string> $results
     * @param array<string, mixed> $metadata
     */
    private function publishError(ThreadSafeArray $results, SleeperNotifier $notifier, int $endpointId, int $id, int $queuedNs, int $code, string $message, array $metadata, int $maxMetadataBytes, ?int $startedNs = null) : void{
        if(strlen($message) > self::MAX_ERROR_MESSAGE_BYTES){
            $message = substr($message, 0, self::MAX_ERROR_MESSAGE_BYTES);
        }
        $waitUs = max(0, intdiv(($startedNs ?? hrtime(true)) - $queuedNs, 1_000));
        $this->publish($results, $notifier, pack('NJJN', $endpointId, $id, $waitUs, $code) . serialize([$message, self::boundedMetadata($metadata, $maxMetadataBytes)]));
    }

    /** @param ThreadSafeArray<int, string> $results */
    private function publish(ThreadSafeArray $results, SleeperNotifier $notifier, string $frame) : void{
        $wake = $results->synchronized(static function() use ($results, $frame) : bool{
            $wake = $results->count() === 0;
            $results[] = $frame;
            return $wake;
        });
        if($wake){
            $notifier->wakeupSleeper();
        }
    }

    /** @param array<string, mixed> $metadata @return array<string, mixed> */
    private static function boundedMetadata(array $metadata, int $maxBytes) : array{
        $bytes = 0;
        foreach($metadata as $key => $values){
            if(!is_string($key) || !is_array($values)){
                return [];
            }
            $bytes += strlen($key);
            foreach($values as $value){
                if(!is_string($value)){
                    return [];
                }
                $bytes += strlen($value);
                if($bytes > $maxBytes){
                    return [];
                }
            }
        }
        return $metadata;
    }

    private function setState(int $state, string $lastError = '') : void{
        $this->synchronized(function() use ($state, $lastError) : void{
            $this->state = $state;
            $this->lastError = $lastError;
        });
    }

    public static function stateName(int $state) : string{
        return match($state){
            self::STATE_STARTING => 'starting',
            self::STATE_READY => 'ready',
            self::STATE_BACKOFF => 'backoff',
            self::STATE_STOPPING => 'stopping',
            self::STATE_FAILED => 'failed',
            self::STATE_STUCK => 'stuck',
            default => 'unknown',
        };
    }

    public static function connectivityName(int $state) : string{
        return match($state){
            self::CONNECTIVITY_IDLE => 'idle',
            self::CONNECTIVITY_CONNECTING => 'connecting',
            self::CONNECTIVITY_READY => 'ready',
            self::CONNECTIVITY_TRANSIENT_FAILURE => 'transient_failure',
            self::CONNECTIVITY_SHUTDOWN => 'shutdown',
            default => 'unknown',
        };
    }

    public function getThreadName() : string{
        return 'pRPC Worker #' . $this->workerId;
    }
}
