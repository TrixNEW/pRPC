<?php

declare(strict_types=1);

namespace Trix\pRPC\thread;

use Grpc\Call;
use Grpc\Channel;
use Grpc\ChannelCredentials;
use Grpc\Timeval;
use pmmp\thread\ThreadSafeArray;
use pocketmine\snooze\SleeperHandlerEntry;
use pocketmine\snooze\SleeperNotifier;
use pocketmine\thread\Thread;
use pocketmine\thread\ThreadException;
use RuntimeException;
use Throwable;
use Trix\pRPC\retry\RpcStatus;
use const Grpc\OP_RECV_INITIAL_METADATA;
use const Grpc\OP_RECV_MESSAGE;
use const Grpc\OP_RECV_STATUS_ON_CLIENT;
use const Grpc\OP_SEND_CLOSE_FROM_CLIENT;
use const Grpc\OP_SEND_INITIAL_METADATA;
use const Grpc\OP_SEND_MESSAGE;

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
    private bool $hung = false;
    private string $lastError = '';
    private int $busyEndpoint = -1;
    private int $busyUntilNs = 0;
    private string $inflight = '';

    /** @param ThreadSafeArray<string, mixed> $gate */
    public function __construct(
        private readonly RpcDispatchQueue $queue,
        private readonly SleeperHandlerEntry $sleeperEntry,
        private readonly ThreadSafeArray $gate,
        public readonly int $gateKey,
        public readonly int $workerId
    ){}

    /** Called by the queue while it holds its lock, the batch isn't running yet so it can't be stuck. */
    public function markBusy(int $endpointId, string $ids) : void{
        $this->synchronized(function() use ($endpointId, $ids) : void{
            $this->busyEndpoint = $endpointId;
            $this->busyUntilNs = 0;
            $this->inflight = $ids;
        });
    }

    private function markRunning(int $untilNs) : void{
        $this->synchronized(function() use ($untilNs) : void{
            $this->busyUntilNs = $untilNs;
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

    public function markHung() : void{
        $this->synchronized(function() : void{
            $this->hung = true;
            $this->state = self::STATE_STUCK;
        });
    }

    public function quit() : void{
        if($this->isRunning()){
            // joining blocks for good on a thread stuck in ext-grpc, failing lets PocketMine finish shutting down
            if($this->synchronized(fn() : bool => $this->hung)){
                throw new ThreadException('pRPC worker ' . $this->workerId . ' is stuck inside ext-grpc and cannot be joined.');
            }
            $this->queue->stop();
        }
        parent::quit();
    }

    protected function onRun() : void{
        $notifier = $this->sleeperEntry->createNotifier();
        $results = $this->queue->results;
        /** @var array<int, array<string, mixed>|null> $configs */
        $configs = [];
        /** @var array<int, Channel> $channels */
        $channels = [];
        /** @var array<int, array{untilNs: int, delayMs: int}> $backoff */
        $backoff = [];
        $epoch = -1;
        $normalExit = false;
        $this->setState(self::STATE_READY);

        try{
            while(($batch = $this->queue->take($this)) !== null){
                [$endpointId, $jobs, $currentEpoch] = $batch;

                if($currentEpoch !== $epoch){
                    $epoch = $currentEpoch;
                    foreach($configs as $id => $_){
                        if($this->queue->laneConfig($id) === null){
                            if(isset($channels[$id])){
                                self::closeChannel($channels[$id]);
                            }
                            unset($configs[$id], $channels[$id], $backoff[$id]);
                        }
                    }
                }
                if(!array_key_exists($endpointId, $configs)){
                    $raw = $this->queue->laneConfig($endpointId);
                    $configs[$endpointId] = $raw === null ? null : unserialize($raw, ['allowed_classes' => false]);
                }

                [$connectivity, $probed] = $this->runBatch($endpointId, $configs[$endpointId], $jobs, $channels, $backoff, $results, $notifier);
                if(!$this->queue->finish($this, $endpointId, $connectivity, $probed)){
                    break;
                }
            }
            $normalExit = true;
        }finally{
            foreach($channels as $channel){
                self::closeChannel($channel);
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
     * @param array<int, Channel> $channels
     * @param array<int, array{untilNs: int, delayMs: int}> $backoff
     * @param ThreadSafeArray<int, string> $results
     * @return array{int, bool} connectivity after the batch and whether it carried a probe
     */
    private function runBatch(int $endpointId, ?array $config, array $jobs, array &$channels, array &$backoff, ThreadSafeArray $results, SleeperNotifier $notifier) : array{
        $probed = false;
        $parsed = [];
        $untilNs = 0;
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
            $untilNs = max($untilNs, $h['deadline']);
        }

        if($config === null){
            foreach($parsed as [$id, , $queued]){
                $this->publishError($results, $notifier, $endpointId, $id, $queued, RpcStatus::UNAVAILABLE, 'RPC endpoint was removed.', [], 0);
            }
            return [self::CONNECTIVITY_UNKNOWN, $probed];
        }

        $now = hrtime(true);
        $channel = $channels[$endpointId] ?? null;
        if($channel === null){
            $wait = $backoff[$endpointId] ?? null;
            $error = null;
            if($wait !== null && $wait['untilNs'] > $now){
                $error = 'gRPC channel is backing off after a failed setup: ' . $this->getLastError();
            }else{
                try{
                    $channel = $channels[$endpointId] = self::createChannel($config);
                    unset($backoff[$endpointId]);
                    $this->setState(self::STATE_READY, '');
                }catch(Throwable $e){
                    $delay = $wait === null ? (int) $config['reconnectBackoffMinMs'] : min($wait['delayMs'] * 2, (int) $config['reconnectBackoffMaxMs']);
                    $backoff[$endpointId] = ['untilNs' => $now + (int) ($delay * (0.8 + mt_rand() / mt_getrandmax() * 0.4)) * 1_000_000, 'delayMs' => $delay];
                    $this->setState(self::STATE_BACKOFF, $e->getMessage());
                    $error = 'Unable to initialize gRPC client: ' . $e->getMessage();
                }
            }
            if($channel === null){
                foreach($parsed as [$id, , $queued]){
                    $this->publishError($results, $notifier, $endpointId, $id, $queued, RpcStatus::UNAVAILABLE, (string) $error, [], (int) $config['maxMetadataBytes']);
                }
                return [self::CONNECTIVITY_TRANSIENT_FAILURE, $probed];
            }
        }

        if($probed){
            try{
                $channel->getConnectivityState(true);
            }catch(Throwable){
            }
        }

        if($parsed !== []){
            GrpcGate::enter($this->gate, $this->gateKey, count($parsed) > 1);
            try{
                $this->markRunning(max($untilNs, hrtime(true)));
                if(count($parsed) === 1){
                    $this->runSingle($endpointId, $config, $parsed[0], $channel, $channels, $results, $notifier);
                }else{
                    $this->runPipelined($endpointId, $config, $parsed, $channel, $channels, $results, $notifier);
                }
            }finally{
                GrpcGate::leave($this->gate, $this->gateKey);
            }
        }

        try{
            return [isset($channels[$endpointId]) ? $channels[$endpointId]->getConnectivityState(false) : self::CONNECTIVITY_TRANSIENT_FAILURE, $probed];
        }catch(Throwable){
            return [self::CONNECTIVITY_UNKNOWN, $probed];
        }
    }

    /**
     * Sends and receives in one batch. ext-grpc reads its completion queue once when the batch starts, so a thread
     * starting during the call can't leave the wait hanging.
     *
     * @param array<string, mixed> $config
     * @param array{int, int, int, int, array<string, list<string>>, string} $job
     * @param array<int, Channel> $channels
     * @param ThreadSafeArray<int, string> $results
     */
    private function runSingle(int $endpointId, array $config, array $job, Channel $channel, array &$channels, ThreadSafeArray $results, SleeperNotifier $notifier) : void{
        [$id, $deadlineNs, $queuedNs, $methodId, $metadata, $requestData] = $job;
        // built first so nothing but the batch call runs between creating the call and waiting on it
        $ops = [
            OP_SEND_INITIAL_METADATA => $metadata,
            OP_SEND_MESSAGE => ['message' => $requestData],
            OP_SEND_CLOSE_FROM_CLIENT => true,
            OP_RECV_INITIAL_METADATA => true,
            OP_RECV_MESSAGE => true,
            OP_RECV_STATUS_ON_CLIENT => true,
        ];
        $startedNs = hrtime(true);
        $call = $this->createCall($endpointId, $config, $channel, $id, $deadlineNs, $queuedNs, $methodId, $results, $notifier);
        if($call === null){
            return;
        }
        try{
            $event = $call->startBatch($ops);
        }catch(Throwable $e){
            $this->callFailed($endpointId, $channels, $id, $queuedNs, $startedNs, $e, $results, $notifier);
            return;
        }
        $this->publishEvent($endpointId, $config, $id, $queuedNs, $startedNs, $event, $results, $notifier);
    }

    /**
     * Starts every call, then waits on each in deadline order. Faster on a slow backend, but a thread starting in the
     * meantime replaces ext-grpc's completion queue and the wait never returns.
     *
     * @param array<string, mixed> $config
     * @param list<array{int, int, int, int, array<string, list<string>>, string}> $parsed
     * @param array<int, Channel> $channels
     * @param ThreadSafeArray<int, string> $results
     */
    private function runPipelined(int $endpointId, array $config, array $parsed, Channel $channel, array &$channels, ThreadSafeArray $results, SleeperNotifier $notifier) : void{
        $calls = [];
        foreach($parsed as [$id, $deadlineNs, $queuedNs, $methodId, $metadata, $requestData]){
            $call = $this->createCall($endpointId, $config, $channel, $id, $deadlineNs, $queuedNs, $methodId, $results, $notifier);
            if($call === null){
                continue;
            }
            $startedNs = hrtime(true);
            try{
                $call->startBatch([
                    OP_SEND_INITIAL_METADATA => $metadata,
                    OP_SEND_MESSAGE => ['message' => $requestData],
                    OP_SEND_CLOSE_FROM_CLIENT => true,
                ]);
                $calls[] = [$id, $deadlineNs, $queuedNs, $startedNs, $call];
            }catch(Throwable $e){
                $this->callFailed($endpointId, $channels, $id, $queuedNs, $startedNs, $e, $results, $notifier);
            }
        }

        usort($calls, static fn(array $a, array $b) : int => $a[1] <=> $b[1]);
        foreach($calls as [$id, , $queuedNs, $startedNs, $call]){
            try{
                $event = $call->startBatch([
                    OP_RECV_INITIAL_METADATA => true,
                    OP_RECV_MESSAGE => true,
                    OP_RECV_STATUS_ON_CLIENT => true,
                ]);
            }catch(Throwable $e){
                $this->callFailed($endpointId, $channels, $id, $queuedNs, $startedNs, $e, $results, $notifier);
                continue;
            }
            $this->publishEvent($endpointId, $config, $id, $queuedNs, $startedNs, $event, $results, $notifier);
        }
    }

    /**
     * @param array<string, mixed> $config
     * @param ThreadSafeArray<int, string> $results
     */
    private function createCall(int $endpointId, array $config, Channel $channel, int $id, int $deadlineNs, int $queuedNs, int $methodId, ThreadSafeArray $results, SleeperNotifier $notifier) : ?Call{
        $now = hrtime(true);
        if($deadlineNs <= $now){
            $this->publishError($results, $notifier, $endpointId, $id, $queuedNs, RpcStatus::DEADLINE_EXCEEDED, 'RPC deadline exceeded before execution.', [], 0);
            return null;
        }
        $path = $config['methodPaths'][$methodId] ?? null;
        if(!is_string($path)){
            $this->publishError($results, $notifier, $endpointId, $id, $queuedNs, RpcStatus::INTERNAL, 'Unknown RPC method ID.', [], 0);
            return null;
        }
        try{
            return new Call($channel, $path, Timeval::now()->add(new Timeval(max(1, intdiv($deadlineNs - $now, 1_000)))));
        }catch(Throwable $e){
            $this->publishError($results, $notifier, $endpointId, $id, $queuedNs, RpcStatus::INTERNAL, 'RPC dispatch failed: ' . $e->getMessage(), [], 0);
            return null;
        }
    }

    /**
     * @param array<string, mixed> $config
     * @param ThreadSafeArray<int, string> $results
     */
    private function publishEvent(int $endpointId, array $config, int $id, int $queuedNs, int $startedNs, object $event, ThreadSafeArray $results, SleeperNotifier $notifier) : void{
        $status = $event->status ?? null;
        $code = is_int($status->code ?? null) ? $status->code : RpcStatus::UNKNOWN;
        $metadata = is_array($status->metadata ?? null) ? $status->metadata : [];
        $maxMetadataBytes = (int) $config['maxMetadataBytes'];
        $message = $event->message ?? null;
        if($code !== RpcStatus::OK){
            $details = is_string($status->details ?? null) ? $status->details : 'Unknown gRPC error.';
            $this->publishError($results, $notifier, $endpointId, $id, $queuedNs, $code, $details, $metadata, $maxMetadataBytes, $startedNs);
        }elseif(!is_string($message)){
            $this->publishError($results, $notifier, $endpointId, $id, $queuedNs, RpcStatus::INTERNAL, 'gRPC returned OK without a response message.', $metadata, $maxMetadataBytes, $startedNs);
        }elseif(strlen($message) > (int) $config['maxResponseBytes']){
            $this->publishError($results, $notifier, $endpointId, $id, $queuedNs, RpcStatus::INTERNAL, 'RPC response exceeded the configured worker limit.', $metadata, $maxMetadataBytes, $startedNs);
        }else{
            $this->publish($results, $notifier, pack('NJJN', $endpointId, $id, intdiv($startedNs - $queuedNs, 1_000), RpcStatus::OK) . $message);
        }
    }

    /**
     * @param array<int, Channel> $channels
     * @param ThreadSafeArray<int, string> $results
     */
    private function callFailed(int $endpointId, array &$channels, int $id, int $queuedNs, int $startedNs, Throwable $e, ThreadSafeArray $results, SleeperNotifier $notifier) : void{
        if(isset($channels[$endpointId])){
            self::closeChannel($channels[$endpointId]);
            unset($channels[$endpointId]);
        }
        $this->publishError($results, $notifier, $endpointId, $id, $queuedNs, RpcStatus::UNAVAILABLE, 'gRPC call failed: ' . $e->getMessage(), [], 0, $startedNs);
    }

    /** @param array<string, mixed> $config */
    private static function createChannel(array $config) : Channel{
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

        return new Channel($endpoint, $channelOptions);
    }

    private static function closeChannel(Channel $channel) : void{
        try{
            $channel->close();
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
