<?php

declare(strict_types=1);

namespace Trix\pRPC\thread;

use Google\Protobuf\Internal\Message;
use Grpc\BaseStub;
use Grpc\ChannelCredentials;
use Grpc\UnaryCall;
use pmmp\thread\ThreadSafeArray;
use pocketmine\snooze\SleeperHandlerEntry;
use pocketmine\snooze\SleeperNotifier;
use pocketmine\thread\Thread;
use RuntimeException;
use Throwable;
use const Grpc\STATUS_DEADLINE_EXCEEDED;
use const Grpc\STATUS_INTERNAL;
use const Grpc\STATUS_OK;
use const Grpc\STATUS_UNKNOWN;

/** @internal */
final class GrpcWorkerThread extends Thread{
    private ThreadSafeArray $queue;
    private ThreadSafeArray $results;

    private bool $stopping = false;
    private bool $stoppedNormally = false;
    private int $outstanding = 0;

    public function __construct(
        private string $serializedConfig,
        private SleeperHandlerEntry $sleeperEntry,
        private int $batchSize,
        private int $workerId
    ){
        $this->queue = new ThreadSafeArray();
        $this->results = new ThreadSafeArray();
    }

    public function enqueue(
        int $id,
        string $requestClass,
        string $requestData,
        string $method,
        array $metadata,
        int $deadlineNs
    ) : void{
        $payload = serialize([
            'id' => $id,
            'requestClass' => $requestClass,
            'requestData' => $requestData,
            'method' => $method,
            'metadata' => $metadata,
            'deadlineNs' => $deadlineNs,
        ]);

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
        $config = $this->decodeArray($this->serializedConfig, 'worker config');
        $client = null;
        $normalExit = false;

        try{
            while(($batch = $this->nextBatch()) !== null){
                if($batch === []){
                    continue;
                }

                try{
                    $client ??= $this->createClient($config);
                }catch(Throwable $e){
                    foreach($batch as $payload){
                        $job = $this->tryDecodeJob($payload);
                        if($job !== null){
                            $this->publishError(
                                (int) $job['id'],
                                STATUS_INTERNAL,
                                'Unable to initialize gRPC client: ' . $e->getMessage(),
                                [],
                                $notifier
                            );
                        }else{
                            $this->decrementOutstanding();
                        }
                    }
                    continue;
                }

                $this->processBatch($batch, $client, $notifier);
            }
            $normalExit = true;
        }finally{
            if($client instanceof BaseStub){
                try{
                    $client->close();
                }catch(Throwable){
                    // ...
                }
            }

            $this->synchronized(function() use ($normalExit) : void{
                $this->stoppedNormally = $normalExit;
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
                if(is_string($payload)){
                    $batch[] = $payload;
                }
                unset($this->queue[$key]);

                if(count($batch) >= $this->batchSize){
                    break;
                }
            }
            return $batch;
        });
    }

    private function processBatch(array $batch, ?BaseStub &$client, SleeperNotifier $notifier) : void{
        /** @var list<array{id: int, call: UnaryCall}> $calls */
        $calls = [];
        $resetClientAfterBatch = false;

        foreach($batch as $payload){
            $job = $this->tryDecodeJob($payload);
            if($job === null){
                $this->decrementOutstanding();
                continue;
            }

            $id = (int) $job['id'];

            try{
                $deadlineNs = (int) $job['deadlineNs'];
                $now = hrtime(true);
                if($deadlineNs <= $now){
                    $this->publishError($id, STATUS_DEADLINE_EXCEEDED, 'RPC deadline exceeded before execution.', [], $notifier);
                    continue;
                }

                $requestClass = $job['requestClass'];
                if(!is_string($requestClass) || !is_a($requestClass, Message::class, true)){
                    throw new RuntimeException('Request class is not a protobuf Message.');
                }

                $method = $job['method'];
                if(!is_string($method) || $method === '' || str_starts_with($method, '__') || !method_exists($client, $method)){
                    throw new RuntimeException('Unknown or invalid gRPC unary method.');
                }

                $request = new $requestClass();
                $request->mergeFromString((string) $job['requestData']);

                $remainingUs = max(1, intdiv($deadlineNs - hrtime(true), 1_000));
                $metadata = is_array($job['metadata'] ?? null) ? $job['metadata'] : [];
                $call = $client->{$method}($request, $metadata, ['timeout' => $remainingUs]);

                if(!$call instanceof UnaryCall){
                    throw new RuntimeException("{$method} is not a unary gRPC method.");
                }

                $calls[] = ['id' => $id, 'call' => $call];
            }catch(Throwable $e){
                $this->publishError($id, STATUS_INTERNAL, 'RPC dispatch failed: ' . $e->getMessage(), [], $notifier);
            }
        }

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

                if(!$response instanceof Message){
                    $this->publishError($id, STATUS_INTERNAL, 'gRPC returned OK without a protobuf response.', $metadata, $notifier);
                    continue;
                }

                $this->publishSuccess($id, $response, $notifier);
            }catch(Throwable $e){
                $resetClientAfterBatch = true;
                $this->publishError($id, STATUS_UNKNOWN, 'gRPC wait failed: ' . $e->getMessage(), [], $notifier);
            }
        }

        if($resetClientAfterBatch){
            try{
                $client->close();
            }catch(Throwable){
            }
            $client = null;
        }
    }

    /** @param array<string, mixed> $config */
    private function createClient(array $config) : BaseStub{
        $class = $config['stubClass'] ?? null;
        $endpoint = $config['endpoint'] ?? null;
        $channelOptions = $config['channelOptions'] ?? null;
        $credentials = $config['credentials'] ?? null;

        if(!is_string($class) || !is_a($class, BaseStub::class, true)){
            throw new RuntimeException('Invalid gRPC stub class.');
        }
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

        $client = new $class($endpoint, $channelOptions);
        if(!$client instanceof BaseStub){
            throw new RuntimeException('Generated client did not produce a BaseStub.');
        }
        return $client;
    }

    private function publishSuccess(int $id, Message $response, SleeperNotifier $notifier) : void{
        try{
            $payload = serialize([
                'id' => $id,
                'ok' => true,
                'responseClass' => $response::class,
                'responseData' => $response->serializeToString(),
            ]);
            $this->publish($payload, $notifier);
        }catch(Throwable $e){
            $this->publishError($id, STATUS_INTERNAL, 'Response serialization failed: ' . $e->getMessage(), [], $notifier);
        }
    }

    /** @param array<string, mixed> $metadata */
    private function publishError(int $id, int $statusCode, string $message, array $metadata, SleeperNotifier $notifier) : void{
        $payload = serialize([
            'id' => $id,
            'ok' => false,
            'statusCode' => $statusCode,
            'message' => $message,
            'metadata' => $metadata,
        ]);
        $this->publish($payload, $notifier);
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

    /** @return array<string, mixed>|null */
    private function tryDecodeJob(string $payload) : ?array{
        try{
            $job = unserialize($payload, ['allowed_classes' => false]);
            return is_array($job) && isset($job['id']) ? $job : null;
        }catch(Throwable){
            return null;
        }
    }

    /** @return array<string, mixed> */
    private function decodeArray(string $payload, string $what) : array{
        $decoded = unserialize($payload, ['allowed_classes' => false]);
        if(!is_array($decoded)){
            throw new RuntimeException("Invalid $what payload.");
        }
        return $decoded;
    }

    public function getThreadName() : string{
        return 'pRPC Worker #' . $this->workerId;
    }
}
