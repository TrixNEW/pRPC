<?php

declare(strict_types=1);

namespace Trix\pRPC\internal;

use InvalidArgumentException;
use pocketmine\plugin\PluginBase;
use pocketmine\scheduler\ClosureTask;
use pocketmine\scheduler\TaskHandler;
use pocketmine\snooze\SleeperHandler;
use pocketmine\snooze\SleeperHandlerEntry;
use pocketmine\thread\ThreadManager;
use RuntimeException;
use Throwable;
use Trix\pRPC\RpcClient;
use Trix\pRPC\thread\GrpcWorkerThread;
use Trix\pRPC\thread\RpcDispatchQueue;
use Trix\pRPC\thread\RpcLane;

/**
 * Worker threads shared by one or more clients. Each client is a lane in the dispatch queue.
 *
 * ext-grpc waits on one completion queue for the whole process and allows at most 6 threads to wait on it at once,
 * a 7th returns early while gRPC still writes into its stack. So a pool never runs more than 6 workers.
 *
 * @internal
 */
final class RpcWorkerPool{
    public const int MAX_WORKERS = 6;
    // gRPC enforces each call's deadline, so a worker this far past its latest one is never coming back
    private const int STUCK_GRACE_NS = 2_000_000_000;
    private const int WATCHDOG_EVERY_TICKS = 10;

    private static int $liveThreads = 0;
    /** @var list<GrpcWorkerThread> busy workers left to finish their call after their pool closed */
    private static array $detached = [];

    private readonly SleeperHandler $sleeper;
    private ?SleeperHandlerEntry $sleeperEntry = null;
    private ?TaskHandler $task = null;
    private readonly RpcDispatchQueue $queue;
    /** @var array<int, GrpcWorkerThread> */
    private array $workers = [];
    /** @var array<int, RpcClient> */
    private array $clients = [];
    private int $nextEndpointId = 1;
    private int $nextWorkerId = 0;
    private int $outstanding = 0;
    private int $outstandingBytes = 0;
    private int $lostWorkers = 0;
    private int $ticks = 0;
    private bool $closed = false;

    public function __construct(
        PluginBase $plugin,
        public readonly int $size,
        public readonly int $maxOutstanding,
        public readonly int $maxQueuedBytes,
        private readonly int $tickInterval = 1
    ){
        if($size < 1 || $size > self::MAX_WORKERS){
            throw new InvalidArgumentException('A worker pool needs between 1 and ' . self::MAX_WORKERS . ' workers.');
        }
        if($maxOutstanding < 1 || $maxQueuedBytes < 1){
            throw new InvalidArgumentException('Pool limits must be greater than 0.');
        }
        if(self::$liveThreads + $size > self::MAX_WORKERS){
            \GlobalLogger::get()->warning('pRPC now runs ' . (self::$liveThreads + $size) . ' worker threads, ext-grpc only supports ' . self::MAX_WORKERS . ' waiting at once. Share one RpcClientManager instead of creating many clients.');
        }

        $this->sleeper = $plugin->getServer()->getTickSleeper();
        $this->queue = new RpcDispatchQueue();
        try{
            $this->sleeperEntry = $this->sleeper->addNotifier(function() : void{
                $this->drainResults();
            });
            for($i = 0; $i < $size; ++$i){
                $this->spawn();
            }
            $this->task = $plugin->getScheduler()->scheduleRepeatingTask(new ClosureTask(function() : void{
                $this->pump(++$this->ticks % self::WATCHDOG_EVERY_TICKS === 0);
            }), $tickInterval);
        }catch(Throwable $e){
            $this->closed = true;
            $this->queue->stop();
            foreach($this->workers as $worker){
                try{
                    $worker->quit();
                }catch(Throwable){
                }
            }
            self::$liveThreads -= count($this->workers);
            $this->workers = [];
            if($this->sleeperEntry !== null){
                $this->sleeper->removeNotifier($this->sleeperEntry->getNotifierId());
            }
            $this->task?->cancel();
            throw $e;
        }
    }

    public function attach(RpcClient $client, string $config, int $cap, int $batchSize) : int{
        if($this->closed){
            throw new RuntimeException('Worker pool is closed.');
        }
        $id = $this->nextEndpointId++;
        $this->clients[$id] = $client;
        $this->queue->addLane($id, new RpcLane($config, max(1, min($cap, $this->size)), $batchSize));
        return $id;
    }

    /** @return list<string> jobs that were still queued */
    public function detach(int $endpointId) : array{
        unset($this->clients[$endpointId]);
        return $this->queue->removeLane($endpointId);
    }

    public function hasClients() : bool{
        return $this->clients !== [];
    }

    public function isFull(int $bytes) : ?string{
        if($this->outstanding >= $this->maxOutstanding){
            return 'request';
        }
        if($this->outstandingBytes + $bytes > $this->maxQueuedBytes){
            return 'byte';
        }
        return null;
    }

    public function submit(int $endpointId, string $job, int $bytes) : bool{
        if($this->closed || !$this->queue->push($endpointId, $job)){
            return false;
        }
        ++$this->outstanding;
        $this->outstandingBytes += $bytes;
        return true;
    }

    public function release(int $bytes) : void{
        --$this->outstanding;
        $this->outstandingBytes -= $bytes;
    }

    public function probe(int $endpointId) : void{
        $this->queue->probe($endpointId);
    }

    public function connectivity(int $endpointId) : int{
        return $this->queue->connectivity($endpointId);
    }

    /** @return array{queued: int, busy: int, connectivity: int, reconnects: int} */
    public function laneStats(int $endpointId) : array{
        return $this->queue->laneStats($endpointId);
    }

    public function liveWorkers() : int{
        return count($this->workers);
    }

    public function lostWorkers() : int{
        return $this->lostWorkers;
    }

    public function outstanding() : int{
        return $this->outstanding;
    }

    /** @return array{list<int>, list<string>} calls in each worker's current batch, and each worker's state */
    public function workerStats() : array{
        $loads = [];
        $states = [];
        foreach($this->workers as $worker){
            $snapshot = $worker->snapshot();
            $loads[] = count($snapshot['ids']);
            $states[] = GrpcWorkerThread::stateName($snapshot['state']);
        }
        return [$loads, $states];
    }

    /** Drains results, ticks every client and, when asked, checks for dead or stuck workers. */
    public function pump(bool $watchdog = true) : void{
        if($this->closed){
            return;
        }
        if($this->outstanding > 0){
            $this->drainResults();
        }
        $now = hrtime(true);
        foreach($this->clients as $client){
            $client->onTick($now);
        }
        if($watchdog && $this->outstanding > 0){
            $this->watchdog($now);
        }
    }

    public function drainResults() : void{
        $results = $this->queue->results;
        while(($frames = $results->chunk(256)) !== []){
            foreach($frames as $frame){
                if(!is_string($frame) || strlen($frame) < GrpcWorkerThread::RESULT_HEADER_BYTES){
                    continue;
                }
                $h = unpack('Nendpoint/Jid/Jwait/Ncode', $frame);
                $client = $this->clients[$h['endpoint']] ?? null;
                $client?->onResult($h['id'], $h['code'], $h['wait'], substr($frame, GrpcWorkerThread::RESULT_HEADER_BYTES));
            }
        }
    }

    private function watchdog(int $now) : void{
        foreach($this->workers as $workerId => $worker){
            $failed = $worker->isTerminated() && !$worker->stoppedNormally();
            $snapshot = $worker->snapshot();
            $stuck = !$failed && $snapshot['endpoint'] >= 0 && $snapshot['untilNs'] > 0 && $now > $snapshot['untilNs'] + self::STUCK_GRACE_NS;
            if(!$failed && !$stuck){
                continue;
            }

            // results it published before failing go first, so only what it really lost is failed below
            $this->drainResults();
            $this->queue->abandon($worker);
            unset($this->workers[$workerId]);
            --self::$liveThreads;
            ++$this->lostWorkers;

            if($failed){
                $lastError = $worker->getLastError();
                $reason = "gRPC worker $workerId terminated unexpectedly." . ($lastError !== '' ? ' Last error: ' . $lastError : '');
                try{
                    $worker->join();
                }catch(Throwable){
                }
                ThreadManager::getInstance()->remove($worker);
            }else{
                // never join it, it would block forever
                $reason = "gRPC worker $workerId stopped responding inside a call and was replaced.";
                self::$detached[] = $worker;
            }
            \GlobalLogger::get()->warning('pRPC: ' . $reason);
            if($snapshot['endpoint'] >= 0){
                ($this->clients[$snapshot['endpoint']] ?? null)?->onTransportLost($snapshot['ids'], $reason);
            }

            if($this->lostWorkers <= $this->size * 2){
                try{
                    $this->spawn();
                }catch(Throwable $e){
                    \GlobalLogger::get()->logException($e);
                }
            }
        }
    }

    private function spawn() : void{
        $worker = new GrpcWorkerThread($this->queue, $this->sleeperEntry ?? throw new RuntimeException('No sleeper entry.'), $this->nextWorkerId++);
        if(!$worker->start()){
            throw new RuntimeException("Failed to start gRPC worker {$worker->workerId}.");
        }
        $this->workers[$worker->workerId] = $worker;
        ++self::$liveThreads;
    }

    /**
     * Stops every client using this pool. Returns after the grace period, workers still inside a call are left to
     * finish it in the background instead of blocking the main thread until its deadline.
     */
    public function close(int $gracePeriodMs) : void{
        if($this->closed){
            return;
        }
        $this->task?->cancel();
        $this->task = null;
        foreach($this->clients as $client){
            $client->markClosed();
        }
        $this->queue->stop();

        $deadline = hrtime(true) + $gracePeriodMs * 1_000_000;
        while($this->outstanding > 0 && hrtime(true) < $deadline){
            $this->drainResults();
            $now = hrtime(true);
            foreach($this->clients as $client){
                $client->onTick($now);
            }
            usleep(250);
        }
        $this->closed = true;

        foreach($this->queue->discardQueued() as $endpointId => $jobs){
            ($this->clients[$endpointId] ?? null)?->onDropped($jobs);
        }
        $this->drainResults();
        foreach($this->clients as $client){
            $client->abandonTransport();
        }
        $this->clients = [];

        foreach($this->workers as $worker){
            if($worker->snapshot()['endpoint'] >= 0){
                self::$detached[] = $worker;
                continue;
            }
            try{
                $worker->quit();
            }catch(Throwable){
            }
        }
        self::$liveThreads -= count($this->workers);
        $this->workers = [];
        self::reapDetached();

        if($this->sleeperEntry !== null){
            $this->sleeper->removeNotifier($this->sleeperEntry->getNotifierId());
            $this->sleeperEntry = null;
        }
    }

    public function isClosed() : bool{
        return $this->closed;
    }

    private static function reapDetached() : void{
        foreach(self::$detached as $i => $worker){
            if($worker->isTerminated()){
                try{
                    $worker->quit();
                }catch(Throwable){
                }
                unset(self::$detached[$i]);
            }
        }
        self::$detached = array_values(self::$detached);
    }
}
