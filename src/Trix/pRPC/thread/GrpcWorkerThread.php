<?php

declare(strict_types=1);

namespace Trix\pRPC\thread;

use Grpc\BaseStub;
use Grpc\ChannelCredentials;
use Grpc\UnaryCall;
use pmmp\thread\ThreadSafeArray;
use pocketmine\snooze\SleeperHandlerEntry;
use pocketmine\snooze\SleeperNotifier;
use pocketmine\thread\Thread;
use RuntimeException;
use Throwable;
use Trix\pRPC\internal\RawGrpcStub;
use Trix\pRPC\internal\RawMessage;
use const Grpc\STATUS_DEADLINE_EXCEEDED;
use const Grpc\STATUS_INTERNAL;
use const Grpc\STATUS_OK;
use const Grpc\STATUS_UNKNOWN;
use const Grpc\STATUS_UNAVAILABLE;

/** @internal */
final class GrpcWorkerThread extends Thread{
    private const MAX_ERROR_MESSAGE_BYTES = 8_192;

    public const STATE_STARTING = 0;
    public const STATE_READY = 1;
    public const STATE_BACKOFF = 2;
    public const STATE_STOPPING = 3;
    public const STATE_FAILED = 4;

    private ThreadSafeArray $queue;
    private ThreadSafeArray $results;

    private bool $stopping = false;
    private bool $stoppedNormally = false;
    private int $outstanding = 0;
    private int $state = self::STATE_STARTING;
    private string $lastError = '';
    private int $maxMetadataBytes = 32_768;

    public function __construct(
        private string $serializedConfig,
        private SleeperHandlerEntry $sleeperEntry,
        private int $batchSize,
        private int $workerId
    ){
        $this->queue = new ThreadSafeArray();
        $this->results = new ThreadSafeArray();
    }

    /** @param array<string, list<string>> $metadata */
    public function enqueue(
        int $id,
        int $methodId,
        string $requestData,
        array $metadata,
        int $deadlineNs
    ) : void{
        $payload = serialize([$id, $methodId, $deadlineNs, $metadata, $requestData]);

        $this->synchronized(function() use ($payload) : void{
            if($this->stopping){
                throw new RuntimeException('gRPC worker is stopping.');
            }
            $this->queue[] = $payload;
            ++$this->outstanding;
            $this->notify();
        });
    }

    public function getOutstanding() : int{
        return $this->synchronized(fn() : int => $this->outstanding);
    }

    public function getState() : int{
        return $this->synchronized(fn() : int => $this->state);
    }

    public function getLastError() : string{
        return $this->synchronized(fn() : string => $this->lastError);
    }

    /** @return list<string> */
    public function drainResults() : array{
        return $this->synchronized(function() : array{
            $results = [];
            foreach($this->results as $key => $value){
                if(is_string($value)){
                    $results[] = $value;
                }
                unset($this->results[$key]);
            }
            return $results;
        });
    }

    public function stoppedNormally() : bool{
        return $this->synchronized(fn() : bool => $this->stoppedNormally);
    }

    public function requestStop(bool $discardQueued = false) : void{
        $this->synchronized(function() use ($discardQueued) : void{
            $this->stopping = true;
            $this->state = self::STATE_STOPPING;

            if($discardQueued){
                $discarded = 0;
                foreach($this->queue as $key => $_){
                    unset($this->queue[$key]);
                    ++$discarded;
                }
                $this->outstanding = max(0, $this->outstanding - $discarded);
            }

            $this->notify();
        });
    }

    public function quit() : void{
        $this->requestStop(true);
        parent::quit();
    }

    protected function onRun() : void{
        $notifier = $this->sleeperEntry->createNotifier();
        $config = $this->decodeConfig($this->serializedConfig);
        $client = null;
        $normalExit = false;
        $backoffMs = (int) $config['reconnectBackoffMinMs'];
        $this->maxMetadataBytes = (int) $config['maxMetadataBytes'];

        try{
            while(true){
                if($client === null){
                    if($this->shouldStopNow()){
                        break;
                    }

                    try{
                        $client = $this->createClient($config);
                        $this->setState(self::STATE_READY, '');
                        $backoffMs = (int) $config['reconnectBackoffMinMs'];
                        $this->maxMetadataBytes = (int) $config['maxMetadataBytes'];
                    }catch(Throwable $e){
                        $this->setState(self::STATE_BACKOFF, $e->getMessage());
                        $this->failQueuedWithoutClient($notifier, $e->getMessage());
                        if(!$this->waitBackoff($backoffMs)){
                            break;
                        }
                        $backoffMs = min($backoffMs * 2, (int) $config['reconnectBackoffMaxMs']);
                        continue;
                    }
                }

                $batch = $this->nextBatch();
                if($batch === null){
                    break;
                }
                if($batch === []){
                    continue;
                }

                if($this->processBatch($batch, $client, $notifier, $config)){
                    try{
                        $client->close();
                    }catch(Throwable){
                    }
                    $client = null;
                    $this->setState(self::STATE_STARTING, '');
                }
            }
            $normalExit = true;
        }finally{
            if($client instanceof BaseStub){
                try{
                    $client->close();
                }catch(Throwable){
                }
            }

            $this->synchronized(function() use ($normalExit) : void{
                $this->stoppedNormally = $normalExit;
                $this->state = $normalExit ? self::STATE_STOPPING : self::STATE_FAILED;
            });
        }
    }

    /** @return list<string>|null */
    private function nextBatch() : ?array{
        return $this->synchronized(function() : ?array{
            while($this->queue->count() === 0 && !$this->stopping){
                $this->wait();
            }

            if($this->queue->count() === 0 && $this->stopping){
                return null;
            }

            $batch = [];
            foreach($this->queue as $key => $payload){
                unset($this->queue[$key]);
                if(is_string($payload)){
                    $batch[] = $payload;
                }else{
                    $this->outstanding = max(0, $this->outstanding - 1);
                }

                if(count($batch) >= $this->batchSize){
                    break;
                }
            }
            return $batch;
        });
    }

    /**
     * @param array<string, mixed> $config
     * @return bool true when the channel should be recreated
     */
    private function processBatch(array $batch, RawGrpcStub $client, SleeperNotifier $notifier, array $config) : bool{
        /** @var list<array{id: int, deadlineNs: int, call: UnaryCall}> $calls */
        $calls = [];
        $resetClient = false;
        /** @var list<string> $methodPaths */
        $methodPaths = $config['methodPaths'];
        $maxRequestBytes = (int) $config['maxRequestBytes'];
        $maxResponseBytes = (int) $config['maxResponseBytes'];

        foreach($batch as $payload){
            $job = $this->decodeJob($payload);
            if($job === null){
                $this->decrementOutstanding();
                continue;
            }

            [$id, $methodId, $deadlineNs, $metadata, $requestData] = $job;

            try{
                $now = hrtime(true);
                if($deadlineNs <= $now){
                    $this->publishError($id, STATUS_DEADLINE_EXCEEDED, 'RPC deadline exceeded before execution.', [], $notifier);
                    continue;
                }
                if(strlen($requestData) > $maxRequestBytes){
                    $this->publishError($id, STATUS_INTERNAL, 'RPC request exceeded the configured worker limit.', [], $notifier);
                    continue;
                }

                $path = $methodPaths[$methodId] ?? null;
                if(!is_string($path)){
                    throw new RuntimeException('Unknown RPC method ID.');
                }

                $remainingUs = max(1, intdiv($deadlineNs - hrtime(true), 1_000));
                $call = $client->rawUnary($path, $requestData, $metadata, ['timeout' => $remainingUs]);
                $calls[] = ['id' => $id, 'deadlineNs' => $deadlineNs, 'call' => $call];
            }catch(Throwable $e){
                $this->publishError($id, STATUS_INTERNAL, 'RPC dispatch failed: ' . $e->getMessage(), [], $notifier);
            }
        }

        usort($calls, static fn(array $a, array $b) : int => $a['deadlineNs'] <=> $b['deadlineNs']);

        foreach($calls as $entry){
            $id = $entry['id'];

            try{
                [$response, $status] = $entry['call']->wait();
                $statusCode = is_int($status->code ?? null) ? $status->code : STATUS_UNKNOWN;
                $details = is_string($status->details ?? null) ? $status->details : 'Unknown gRPC error.';
                $metadata = is_array($status->metadata ?? null) ? $status->metadata : [];

                if($statusCode !== STATUS_OK){
                    $this->publishError($id, $statusCode, $details, $metadata, $notifier);
                    continue;
                }

                if(!$response instanceof RawMessage){
                    $this->publishError($id, STATUS_INTERNAL, 'gRPC returned OK without a raw protobuf response.', $metadata, $notifier);
                    continue;
                }

                $responseData = $response->bytes();
                if(strlen($responseData) > $maxResponseBytes){
                    $this->publishError($id, STATUS_INTERNAL, 'RPC response exceeded the configured worker limit.', $metadata, $notifier);
                    continue;
                }

                $this->publishSuccess($id, $responseData, $notifier);
            }catch(Throwable $e){
                $resetClient = true;
                $this->publishError($id, STATUS_UNAVAILABLE, 'gRPC wait failed: ' . $e->getMessage(), [], $notifier);
            }
        }

        return $resetClient;
    }

    /** @param array<string, mixed> $config
     * @throws \Exception
     */
    private function createClient(array $config) : RawGrpcStub{
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

    private function publishSuccess(int $id, string $responseData, SleeperNotifier $notifier) : void{
        $this->publish(serialize([$id, true, $responseData]), $notifier);
    }

    /** @param array<string, mixed> $metadata */
    private function publishError(int $id, int $statusCode, string $message, array $metadata, SleeperNotifier $notifier) : void{
        if(strlen($message) > self::MAX_ERROR_MESSAGE_BYTES){
            $message = substr($message, 0, self::MAX_ERROR_MESSAGE_BYTES);
        }
        $this->publish(serialize([$id, false, $statusCode, $message, $this->boundedMetadata($metadata)]), $notifier);
    }

    /** @param array<string, mixed> $metadata @return array<string, mixed> */
    private function boundedMetadata(array $metadata) : array{
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
                if($bytes > $this->maxMetadataBytes){
                    return [];
                }
            }
        }
        return $metadata;
    }

    private function publish(string $payload, SleeperNotifier $notifier) : void{
        $this->synchronized(function() use ($payload) : void{
            $this->results[] = $payload;
            $this->outstanding = max(0, $this->outstanding - 1);
        });
        $notifier->wakeupSleeper();
    }

    private function decrementOutstanding() : void{
        $this->synchronized(function() : void{
            $this->outstanding = max(0, $this->outstanding - 1);
        });
    }

    private function failQueuedWithoutClient(SleeperNotifier $notifier, string $reason) : void{
        while(($batch = $this->takeAvailableBatch()) !== []){
            foreach($batch as $payload){
                $job = $this->decodeJob($payload);
                if($job === null){
                    $this->decrementOutstanding();
                    continue;
                }
                $this->publishError($job[0], STATUS_UNAVAILABLE, 'Unable to initialize gRPC client: ' . $reason, [], $notifier);
            }
        }
    }

    /** @return list<string> */
    private function takeAvailableBatch() : array{
        return $this->synchronized(function() : array{
            $batch = [];
            foreach($this->queue as $key => $payload){
                unset($this->queue[$key]);
                if(is_string($payload)){
                    $batch[] = $payload;
                }else{
                    $this->outstanding = max(0, $this->outstanding - 1);
                }
                if(count($batch) >= $this->batchSize){
                    break;
                }
            }
            return $batch;
        });
    }

    private function waitBackoff(int $milliseconds) : bool{
        $deadlineNs = hrtime(true) + ($milliseconds * 1_000_000);
        return $this->synchronized(function() use ($deadlineNs) : bool{
            while(!$this->stopping){
                $remainingNs = $deadlineNs - hrtime(true);
                if($remainingNs <= 0){
                    return true;
                }
                $this->wait(max(1, intdiv($remainingNs, 1_000)));
            }
            return false;
        });
    }

    private function shouldStopNow() : bool{
        return $this->synchronized(fn() : bool => $this->stopping && $this->queue->count() === 0);
    }

    private function setState(int $state, string $lastError) : void{
        $this->synchronized(function() use ($state, $lastError) : void{
            $this->state = $state;
            $this->lastError = $lastError;
        });
    }

    /**
     * @return array{0: int, 1: int, 2: int, 3: array<string, list<string>>, 4: string}|null
     */
    private function decodeJob(string $payload) : ?array{
        try{
            $job = unserialize($payload, ['allowed_classes' => false]);
        }catch(Throwable){
            return null;
        }

        if(!is_array($job) || count($job) !== 5){
            return null;
        }
        [$id, $methodId, $deadlineNs, $metadata, $requestData] = $job;
        if(!is_int($id) || !is_int($methodId) || !is_int($deadlineNs) || !is_array($metadata) || !is_string($requestData)){
            return null;
        }
        return [$id, $methodId, $deadlineNs, $metadata, $requestData];
    }

    /** @return array<string, mixed> */
    private function decodeConfig(string $payload) : array{
        $decoded = unserialize($payload, ['allowed_classes' => false]);
        if(!is_array($decoded)){
            throw new RuntimeException('Invalid gRPC worker configuration payload.');
        }
        foreach(['endpoint', 'credentials', 'channelOptions', 'methodPaths', 'maxRequestBytes', 'maxResponseBytes', 'maxMetadataBytes', 'reconnectBackoffMinMs', 'reconnectBackoffMaxMs'] as $key){
            if(!array_key_exists($key, $decoded)){
                throw new RuntimeException("Missing gRPC worker configuration key '{$key}'.");
            }
        }
        if(!is_array($decoded['methodPaths'])){
            throw new RuntimeException('Invalid method path registry.');
        }
        return $decoded;
    }

    public static function stateName(int $state) : string{
        return match($state){
            self::STATE_STARTING => 'starting',
            self::STATE_READY => 'ready',
            self::STATE_BACKOFF => 'backoff',
            self::STATE_STOPPING => 'stopping',
            self::STATE_FAILED => 'failed',
            default => 'unknown',
        };
    }

    public function getThreadName() : string{
        return 'pRPC Worker #' . $this->workerId;
    }
}
