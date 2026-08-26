<?php

declare(strict_types=1);

namespace Trix\pRPC;

use Google\Protobuf\Internal\Message;
use InvalidArgumentException;
use pocketmine\plugin\PluginBase;
use pocketmine\scheduler\ClosureTask;
use pocketmine\scheduler\TaskHandler;
use pocketmine\snooze\SleeperHandler;
use pocketmine\snooze\SleeperHandlerEntry;
use RuntimeException;
use SplPriorityQueue;
use Throwable;
use Trix\pRPC\exception\RpcClosedException;
use Trix\pRPC\exception\RpcException;
use Trix\pRPC\exception\RpcOverloadedException;
use Trix\pRPC\exception\RpcTimeoutException;
use Trix\pRPC\exception\RpcWorkerException;
use Trix\pRPC\thread\GrpcWorkerThread;
use const Grpc\STATUS_INTERNAL;

final class RpcClient{
    private readonly SleeperHandler $sleeper;
    private ?SleeperHandlerEntry $sleeperEntry = null;
    private ?TaskHandler $timeoutTask = null;

    /** @var list<GrpcWorkerThread> */
    private array $workers = [];

    /** @var array<int, array{resolver: RpcPromiseResolver<Message>, deadlineNs: int, timeoutMs: int, worker: int}> */
    private array $pending = [];

    /** @var SplPriorityQueue<int, array{id: int, deadlineNs: int}> */
    private SplPriorityQueue $deadlines;

    /** @var array<int, true> */
    private array $failedWorkers = [];

    private int $nextId = 1;
    private bool $closed = false;

    /**
     * @throws Throwable
     */
    public function __construct(PluginBase $plugin, private readonly RpcClientConfig $config){
        $this->deadlines = new SplPriorityQueue();
        $this->deadlines->setExtractFlags(SplPriorityQueue::EXTR_DATA);
        $this->sleeper = $plugin->getServer()->getTickSleeper();

        $workerConfig = serialize([
            'endpoint' => $config->endpoint,
            'stubClass' => $config->stubClass,
            'credentials' => $config->credentials->export(),
            'channelOptions' => $config->channelOptions,
        ]);

        try{
            $this->sleeperEntry = $this->sleeper->addNotifier(function() : void{
                if(!$this->closed){
                    $this->drainResults();
                }
            });

            for($i = 0; $i < $config->workers; ++$i){
                $worker = new GrpcWorkerThread($workerConfig, $this->sleeperEntry, $config->batchSize, $i);
                if(!$worker->start()){
                    throw new RuntimeException("Failed to start gRPC worker {$i}.");
                }
                $this->workers[] = $worker;
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
     * Executes a generated unary gRPC client method off the main thread.
     *
     * @template TResponse of Message
     * @return RpcPromise<TResponse>
     */
    public function unary(string $method, Message $request, ?RpcCallOptions $options = null) : RpcPromise{
        if($this->closed){
            return $this->rejected(new RpcClosedException());
        }
        if($method === '' || str_starts_with($method, '__') || !method_exists($this->config->stubClass, $method)){
            throw new InvalidArgumentException("Unknown or invalid gRPC unary method '$method'.");
        }
        if(count($this->pending) >= $this->config->maxPending){
            return $this->rejected(new RpcOverloadedException($this->config->maxPending));
        }

        $options ??= new RpcCallOptions();
        $timeoutMs = $options->timeoutMs ?? $this->config->defaultTimeoutMs;
        if($timeoutMs > $this->config->maxTimeoutMs){
            throw new InvalidArgumentException("timeoutMs cannot exceed {$this->config->maxTimeoutMs} ms.");
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

        $id = $this->allocateId();
        $deadlineNs = hrtime(true) + ($timeoutMs * 1_000_000);

        /** @var RpcPromiseResolver<Message> $resolver */
        $resolver = new RpcPromiseResolver();
        $this->pending[$id] = [
            'resolver' => $resolver,
            'deadlineNs' => $deadlineNs,
            'timeoutMs' => $timeoutMs,
            'worker' => $workerIndex,
        ];
        $this->deadlines->insert(['id' => $id, 'deadlineNs' => $deadlineNs], -$deadlineNs);

        try{
            $this->workers[$workerIndex]->enqueue(
                $id,
                $request::class,
                $requestData,
                $method,
                $options->metadata,
                $deadlineNs
            );
        }catch(Throwable $e){
            unset($this->pending[$id]);
            $resolver->reject(new RpcWorkerException('Failed to queue RPC: ' . $e->getMessage()));
        }

        return $resolver->promise();
    }

    public function pendingCount() : int{
        return count($this->pending);
    }

    public function isClosed() : bool{
        return $this->closed;
    }

    /**
     * Stops accepting work, drains queued RPCs for up to the grace period, joins workers,
     * removes the sleeper notifier and rejects anything that could not finish.
     */
    public function close(int $gracePeriodMs = 1_000) : void{
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
        while($this->hasOutstandingWorkerWork() && hrtime(true) < $graceDeadline){
            $this->drainResults();
            $this->expireDeadlines();
            usleep(500);
        }

        foreach($this->workers as $worker){
            if($worker->getOutstanding() > 0){
                $worker->requestStop(true);
            }
        }

        foreach($this->workers as $worker){
            try{
                $worker->quit();
            }catch(Throwable){
            }
        }

        $this->drainResults();
        $this->rejectAllPending(new RpcClosedException('RPC client closed before the request completed.'));
        $this->workers = [];

        if($this->sleeperEntry !== null){
            $this->sleeper->removeNotifier($this->sleeperEntry->getNotifierId());
            $this->sleeperEntry = null;
        }

        $this->deadlines = new SplPriorityQueue();
        $this->deadlines->setExtractFlags(SplPriorityQueue::EXTR_DATA);
    }

    private function tick() : void{
        $this->drainResults();
        $this->expireDeadlines();
        $this->detectWorkerFailures();
    }

    private function drainResults() : void{
        foreach($this->workers as $worker){
            foreach($worker->drainResults() as $payload){
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
        if(!is_array($result) || !isset($result['id'])){
            return;
        }

        $id = (int) $result['id'];
        $pending = $this->pending[$id] ?? null;
        if($pending === null){
            return;
        }

        unset($this->pending[$id]);
        $resolver = $pending['resolver'];

        if(($result['ok'] ?? false) !== true){
            $resolver->reject(new RpcException(
                is_string($result['message'] ?? null) ? $result['message'] : 'Unknown gRPC error.',
                is_int($result['statusCode'] ?? null) ? $result['statusCode'] : STATUS_INTERNAL,
                is_array($result['metadata'] ?? null) ? $result['metadata'] : []
            ));
            return;
        }

        try{
            $responseClass = $result['responseClass'] ?? null;
            $responseData = $result['responseData'] ?? null;
            if(!is_string($responseClass) || !is_a($responseClass, Message::class, true) || !is_string($responseData)){
                throw new RuntimeException('Worker returned an invalid protobuf response frame.');
            }

            $response = new $responseClass();
            $response->mergeFromString($responseData);
            $resolver->resolve($response);
        }catch(Throwable $e){
            $resolver->reject(new RpcException('Response decode failed: ' . $e->getMessage(), STATUS_INTERNAL));
        }
    }

    private function expireDeadlines() : void{
        $now = hrtime(true);

        while(!$this->deadlines->isEmpty()){
            /** @var array{id: int, deadlineNs: int} $entry */
            $entry = $this->deadlines->current();
            if($entry['deadlineNs'] > $now){
                break;
            }

            $entry = $this->deadlines->extract();
            $pending = $this->pending[$entry['id']] ?? null;
            if($pending === null || $pending['deadlineNs'] !== $entry['deadlineNs']){
                continue;
            }

            unset($this->pending[$entry['id']]);
            $pending['resolver']->reject(new RpcTimeoutException($pending['timeoutMs']));
        }
    }

    private function detectWorkerFailures() : void{
        foreach($this->workers as $index => $worker){
            if(isset($this->failedWorkers[$index]) || !$worker->isTerminated() || $worker->stoppedNormally()){
                continue;
            }

            $this->failedWorkers[$index] = true;
            $error = new RpcWorkerException("gRPC worker $index terminated unexpectedly.");

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
        $lowestOutstanding = PHP_INT_MAX;

        foreach($this->workers as $index => $worker){
            if(isset($this->failedWorkers[$index]) || $worker->isTerminated()){
                continue;
            }

            $outstanding = $worker->getOutstanding();
            if($outstanding < $lowestOutstanding){
                $lowestOutstanding = $outstanding;
                $selected = $index;
                if($outstanding === 0){
                    break;
                }
            }
        }

        return $selected;
    }

    private function allocateId() : int{
        if($this->nextId === PHP_INT_MAX){
            if($this->pending !== []){
                throw new RuntimeException('RPC request ID space exhausted.');
            }
            $this->nextId = 1;
        }
        return $this->nextId++;
    }

    private function hasOutstandingWorkerWork() : bool{
        return array_any($this->workers, fn($worker) => $worker->getOutstanding() > 0);
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
        $resolver = new RpcPromiseResolver();
        $resolver->reject($error);
        return $resolver->promise();
    }
}
