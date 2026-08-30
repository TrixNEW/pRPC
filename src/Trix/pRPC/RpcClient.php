<?php

declare(strict_types=1);

namespace Trix\pRPC;

use Closure;
use Google\Protobuf\Internal\Message;
use InvalidArgumentException;
use pocketmine\plugin\PluginBase;
use pocketmine\scheduler\ClosureTask;
use pocketmine\scheduler\TaskHandler;
use pocketmine\snooze\SleeperHandler;
use pocketmine\snooze\SleeperHandlerEntry;
use RuntimeException;
use Throwable;
use Trix\pRPC\opts\RpcCallOptions;
use Trix\pRPC\opts\RpcClientConfig;
use Trix\pRPC\stats\RpcClientStats;
use Trix\pRPC\thread\GrpcWorkerThread;
use Trix\pRPC\utils\exception\RpcClosedException;
use Trix\pRPC\utils\exception\RpcException;
use Trix\pRPC\utils\exception\RpcMessageTooLargeException;
use Trix\pRPC\utils\exception\RpcOverloadedException;
use Trix\pRPC\utils\exception\RpcTimeoutException;
use Trix\pRPC\utils\exception\RpcWorkerException;
use Trix\pRPC\utils\promise\RpcPromise;
use Trix\pRPC\utils\promise\RpcPromiseResolver;
use const Grpc\STATUS_INTERNAL;

final class RpcClient{
    private readonly SleeperHandler $sleeper;
    private ?SleeperHandlerEntry $sleeperEntry = null;
    private ?TaskHandler $timeoutTask = null;

    /** @var list<GrpcWorkerThread> */
    private array $workers = [];

    /** @var list<int> */
    private array $workerLoads = [];

    /** @var list<int> */
    private array $workerStates = [];

    /** @var array<int, true> */
    private array $failedWorkers = [];

    /** @var array<string, int> */
    private array $methodIdsByPath = [];

    /** @var array<string, RpcMethod> */
    private array $methodsByPath = [];

    /**
     * @var array<int, array{
     *     resolver: RpcPromiseResolver<Message>,
     *     responseClass: class-string<Message>,
     *     deadlineNs: int,
     *     timeoutMs: int,
     *     worker: int
     * }>
     */
    private array $pending = [];

    private int $transportOutstanding = 0;
    private int $nextId = 1;
    private bool $closed = false;

    /** @var Closure(Throwable): void */
    private Closure $callbackErrorHandler;

    /**
     * @throws Throwable
     */
    public function __construct(PluginBase $plugin, private readonly RpcClientConfig $config){
        $this->sleeper = $plugin->getServer()->getTickSleeper();
        $this->callbackErrorHandler = static function(Throwable $e) : void{
            \GlobalLogger::get()->logException($e);
        };

        $methodPaths = [];
        foreach($config->methods as $id => $method){
            $this->methodIdsByPath[$method->path] = $id;
            $this->methodsByPath[$method->path] = $method;
            $methodPaths[$id] = $method->path;
        }

        $workerConfig = serialize([
            'endpoint' => $config->endpoint,
            'credentials' => $config->credentials->export(),
            'channelOptions' => $config->channelOptions,
            'methodPaths' => $methodPaths,
            'maxRequestBytes' => $config->maxRequestBytes,
            'maxResponseBytes' => $config->maxResponseBytes,
            'maxMetadataBytes' => $config->maxMetadataBytes,
            'reconnectBackoffMinMs' => $config->reconnectBackoffMinMs,
            'reconnectBackoffMaxMs' => $config->reconnectBackoffMaxMs,
        ]);

        try{
            $this->sleeperEntry = $this->sleeper->addNotifier(function() : void{
                if($this->workers !== []){
                    $this->drainResults();
                }
            });

            for($i = 0; $i < $config->workers; ++$i){
                $worker = new GrpcWorkerThread($workerConfig, $this->sleeperEntry, $config->batchSize, $i);
                if(!$worker->start()){
                    throw new RuntimeException("Failed to start gRPC worker {$i}.");
                }
                $this->workers[] = $worker;
                $this->workerLoads[] = 0;
                $this->workerStates[] = GrpcWorkerThread::STATE_STARTING;
            }

            $this->timeoutTask = $plugin->getScheduler()->scheduleRepeatingTask(
                new ClosureTask(function() : void{
                    if(!$this->closed){
                        $this->tick();
                    }
                }),
                1
            );
        }catch(Throwable $e){
            $this->closed = true;
            foreach($this->workers as $worker){
                try{
                    $worker->quit();
                }catch(Throwable){
                }
            }
            $this->workers = [];

            if($this->sleeperEntry !== null){
                $this->sleeper->removeNotifier($this->sleeperEntry->getNotifierId());
                $this->sleeperEntry = null;
            }
            $this->timeoutTask?->cancel();
            $this->timeoutTask = null;
            throw $e;
        }
    }

    /**
     * Executes a registered unary RPC without blocking the PocketMine main thread.
     * The request is encoded once on the main thread and the response is decoded once on the main thread.
     *
     * @template TRequest of Message
     * @template TResponse of Message
     * @param RpcMethod<TRequest, TResponse> $method
     * @param TRequest $request
     * @return RpcPromise<TResponse>
     */
    public function call(RpcMethod $method, Message $request, ?RpcCallOptions $options = null) : RpcPromise{
        if($this->closed){
            return $this->rejected(new RpcClosedException());
        }

        $registered = $this->methodsByPath[$method->path] ?? null;
        if($registered === null || $registered->requestClass !== $method->requestClass || $registered->responseClass !== $method->responseClass){
            throw new InvalidArgumentException("RPC method '$method->path' is not registered in this client.");
        }
        if(!$request instanceof $method->requestClass){
            throw new InvalidArgumentException("RPC '$method->path' expects $method->requestClass, got " . $request::class . '.');
        }
        if($this->transportOutstanding >= $this->config->maxOutstanding){
            return $this->rejected(new RpcOverloadedException($this->config->maxOutstanding));
        }

        $options ??= new RpcCallOptions();
        $timeoutMs = $options->timeoutMs ?? $this->config->defaultTimeoutMs;
        if($timeoutMs > $this->config->maxTimeoutMs){
            throw new InvalidArgumentException("timeoutMs cannot exceed {$this->config->maxTimeoutMs} ms.");
        }
        $metadataBytes = $options->metadataBytes();
        if($metadataBytes > $this->config->maxMetadataBytes){
            return $this->rejected(new RpcMessageTooLargeException('metadata', $metadataBytes, $this->config->maxMetadataBytes));
        }

        $workerIndex = $this->selectWorker();
        if($workerIndex === null){
            return $this->rejected(new RpcWorkerException('No healthy gRPC workers are available.'));
        }

        try{
            $requestData = $request->serializeToString();
        }catch(Throwable $e){
            return $this->rejected(new RpcException('Request serialization failed: ' . $e->getMessage(), STATUS_INTERNAL));
        }
        $requestBytes = strlen($requestData);
        if($requestBytes > $this->config->maxRequestBytes){
            return $this->rejected(new RpcMessageTooLargeException('request', $requestBytes, $this->config->maxRequestBytes));
        }

        $id = $this->allocateId();
        $deadlineNs = hrtime(true) + ($timeoutMs * 1_000_000);
        $methodId = $this->methodIdsByPath[$method->path];

        /** @var RpcPromiseResolver<Message> $resolver */
        $resolver = new RpcPromiseResolver($this->callbackErrorHandler);
        $this->pending[$id] = [
            'resolver' => $resolver,
            'responseClass' => $method->responseClass,
            'deadlineNs' => $deadlineNs,
            'timeoutMs' => $timeoutMs,
            'worker' => $workerIndex,
        ];

        try{
            $this->workers[$workerIndex]->enqueue(
                $id,
                $methodId,
                $requestData,
                $options->metadata,
                $deadlineNs
            );
            ++$this->workerLoads[$workerIndex];
            ++$this->transportOutstanding;
        }catch(Throwable $e){
            unset($this->pending[$id]);
            $resolver->reject(new RpcWorkerException('Failed to queue RPC: ' . $e->getMessage()));
        }

        return $resolver->promise();
    }

    public function pendingCount() : int{
        return count($this->pending);
    }

    /** Counts jobs we've already timed out locally but the worker hasn't released yet. */
    public function outstandingCount() : int{
        return $this->transportOutstanding;
    }

    public function isClosed() : bool{
        return $this->closed;
    }

    public function stats() : RpcClientStats{
        return new RpcClientStats(
            count($this->pending),
            $this->transportOutstanding,
            $this->workerLoads,
            array_map(GrpcWorkerThread::stateName(...), $this->workerStates)
        );
    }

    /**
     * Stops taking new work and asks workers to wrap up quickly.
     *
     * PHP gRPC's UnaryCall::wait() blocks inside the worker, so we can't forcibly interrupt
     * a call already running on another thread — worst case we wait out its full RPC deadline.
     * Keep maxTimeoutMs small for DB traffic (default 5s).
     */
    public function close(int $gracePeriodMs = 500) : void{
        if($this->closed){
            return;
        }
        if($gracePeriodMs < 0){
            throw new InvalidArgumentException('gracePeriodMs cannot be negative.');
        }

        $this->closed = true;
        $this->timeoutTask?->cancel();
        $this->timeoutTask = null;

        foreach($this->workers as $worker){
            $worker->requestStop();
        }

        $graceDeadline = hrtime(true) + ($gracePeriodMs * 1_000_000);
        while($this->transportOutstanding > 0 && hrtime(true) < $graceDeadline){
            $this->drainResults();
            $this->expireDeadlines();
            usleep(250);
        }

        foreach($this->workers as $worker){
            $worker->requestStop(true);
        }

        $this->rejectAllPending(new RpcClosedException('RPC client closed before the request completed.'));

        foreach($this->workers as $worker){
            try{
                $worker->quit();
            }catch(Throwable){
            }
        }

        $this->drainResults();
        $this->workers = [];
        $this->workerLoads = [];
        $this->workerStates = [];
        $this->transportOutstanding = 0;

        if($this->sleeperEntry !== null){
            $this->sleeper->removeNotifier($this->sleeperEntry->getNotifierId());
            $this->sleeperEntry = null;
        }
    }

    private function tick() : void{
        $this->drainResults();
        $this->expireDeadlines();
        $this->refreshWorkerStates();
        $this->detectWorkerFailures();
    }

    private function drainResults() : void{
        foreach($this->workers as $workerIndex => $worker){
            foreach($worker->drainResults() as $payload){
                $this->releaseTransportSlot($workerIndex);
                $this->handleResult($payload);
            }
        }
    }

    private function handleResult(string $payload) : void{
        try{
            $result = unserialize($payload, ['allowed_classes' => false]);
        }catch(Throwable){
            return;
        }
        if(!is_array($result) || !isset($result[0]) || !is_int($result[0])){
            return;
        }

        $id = $result[0];
        $pending = $this->pending[$id] ?? null;
        if($pending === null){
            return;
        }

        unset($this->pending[$id]);
        $resolver = $pending['resolver'];

        if(($result[1] ?? null) !== true){
            $resolver->reject(new RpcException(
                is_string($result[3] ?? null) ? $result[3] : 'Unknown gRPC error.',
                is_int($result[2] ?? null) ? $result[2] : STATUS_INTERNAL,
                is_array($result[4] ?? null) ? $result[4] : []
            ));
            return;
        }

        $responseData = $result[2] ?? null;
        if(!is_string($responseData)){
            $resolver->reject(new RpcException('Worker returned an invalid protobuf response frame.', STATUS_INTERNAL));
            return;
        }

        $responseBytes = strlen($responseData);
        if($responseBytes > $this->config->maxResponseBytes){
            $resolver->reject(new RpcMessageTooLargeException('response', $responseBytes, $this->config->maxResponseBytes));
            return;
        }

        try{
            $responseClass = $pending['responseClass'];
            $response = new $responseClass();
            $response->mergeFromString($responseData);
        }catch(Throwable $e){
            $resolver->reject(new RpcException('Response decode failed: ' . $e->getMessage(), STATUS_INTERNAL));
            return;
        }

        $resolver->resolve($response);
    }

    /**
     * A bounded linear scan is cheaper and simpler than retaining stale SplPriorityQueue entries.
     * At the default 4096-outstanding cap this is at most ~82k integer comparisons/sec at 20 TPS.
     */
    private function expireDeadlines() : void{
        if($this->pending === []){
            return;
        }

        $now = hrtime(true);
        foreach($this->pending as $id => $pending){
            if($pending['deadlineNs'] > $now){
                continue;
            }
            unset($this->pending[$id]);
            $pending['resolver']->reject(new RpcTimeoutException($pending['timeoutMs']));
        }
    }

    private function refreshWorkerStates() : void{
        foreach($this->workers as $index => $worker){
            if(isset($this->failedWorkers[$index]) || $worker->isTerminated()){
                continue;
            }
            $this->workerStates[$index] = $worker->getState();
        }
    }

    private function detectWorkerFailures() : void{
        foreach($this->workers as $index => $worker){
            if(isset($this->failedWorkers[$index]) || !$worker->isTerminated() || $worker->stoppedNormally()){
                continue;
            }

            $this->failedWorkers[$index] = true;
            $this->workerStates[$index] = GrpcWorkerThread::STATE_FAILED;

            foreach($worker->drainResults() as $payload){
                $this->releaseTransportSlot($index);
                $this->handleResult($payload);
            }

            $lostSlots = $this->workerLoads[$index] ?? 0;
            $this->workerLoads[$index] = 0;
            $this->transportOutstanding = max(0, $this->transportOutstanding - $lostSlots);

            $lastError = $worker->getLastError();
            $message = "gRPC worker $index terminated unexpectedly.";
            if($lastError !== ''){
                $message .= ' Last error: ' . $lastError;
            }
            $error = new RpcWorkerException($message);

            foreach($this->pending as $id => $pending){
                if($pending['worker'] === $index){
                    unset($this->pending[$id]);
                    $pending['resolver']->reject($error);
                }
            }
        }
    }

    private function selectWorker() : ?int{
        $selected = null;
        $lowestLoad = PHP_INT_MAX;

        foreach($this->workerLoads as $index => $load){
            if(isset($this->failedWorkers[$index])){
                continue;
            }

            $state = $this->workerStates[$index] ?? GrpcWorkerThread::STATE_STARTING;
            if($state === GrpcWorkerThread::STATE_BACKOFF || $state === GrpcWorkerThread::STATE_STOPPING || $state === GrpcWorkerThread::STATE_FAILED){
                continue;
            }

            if($load < $lowestLoad){
                $lowestLoad = $load;
                $selected = $index;
                if($load === 0){
                    break;
                }
            }
        }

        return $selected;
    }

    private function releaseTransportSlot(int $workerIndex) : void{
        if(($this->workerLoads[$workerIndex] ?? 0) > 0){
            --$this->workerLoads[$workerIndex];
        }
        if($this->transportOutstanding > 0){
            --$this->transportOutstanding;
        }
    }

    private function allocateId() : int{
        if($this->nextId === PHP_INT_MAX){
            if($this->transportOutstanding !== 0){
                throw new RuntimeException('RPC request ID space exhausted.');
            }
            $this->nextId = 1;
        }
        return $this->nextId++;
    }

    private function rejectAllPending(Throwable $error) : void{
        $pending = $this->pending;
        $this->pending = [];
        foreach($pending as $entry){
            $entry['resolver']->reject($error);
        }
    }

    /** @template TValue @return RpcPromise<TValue> */
    private function rejected(Throwable $error) : RpcPromise{
        /** @var RpcPromiseResolver<TValue> $resolver */
        $resolver = new RpcPromiseResolver($this->callbackErrorHandler);
        $resolver->reject($error);
        return $resolver->promise();
    }
}
