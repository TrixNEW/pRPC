<?php

declare(strict_types=1);

namespace Trix\pRPC\internal;

use InvalidArgumentException;
use pmmp\thread\ThreadSafeArray;
use pocketmine\plugin\PluginBase;
use pocketmine\scheduler\AsyncPool;
use pocketmine\scheduler\ClosureTask;
use pocketmine\scheduler\TaskHandler;
use pocketmine\snooze\SleeperHandler;
use pocketmine\snooze\SleeperHandlerEntry;
use RuntimeException;
use Throwable;
use Trix\pRPC\RpcClient;
use Trix\pRPC\thread\GrpcGate;
use Trix\pRPC\thread\GrpcWorkerThread;
use Trix\pRPC\thread\RpcDispatchQueue;
use Trix\pRPC\thread\RpcLane;

/**
 * Worker threads shared by one or more clients. Each client is a lane in the dispatch queue.
 *
 * ext-grpc waits on one completion queue for the whole process, see GrpcGate for how pools share it safely.
 *
 * @internal
 */
final class RpcWorkerPool{
    public const int MAX_WORKERS = GrpcGate::MAX_CALLS;
    // gRPC enforces each call's deadline, so a worker this far past its latest one is never coming back
    private const int STUCK_GRACE_NS = 2_000_000_000;
    private const int WATCHDOG_EVERY_TICKS = 10;
    private const int STOP_WAIT_NS = 500_000_000;
    private const int START_WAIT_NS = 250_000_000;
    private const int RESPAWN_EVERY_NS = 1_000_000_000;
    // a hung thread never exits and holds its memory, past this many lost ones are no longer replaced
    private const int MAX_HUNG_THREADS = 16;
    // PocketMine stops async workers idle for 5 minutes, wall time so a lagging server still makes it
    private const int KEEP_ASYNC_EVERY_NS = 120_000_000_000;

    /**
     * Workers that are dead, hung, or still finishing a call after their pool closed. A thread tears down its objects
     * only when joined, so they're joined later once they stopped running, never mid traffic.
     *
     * @var array<int, array{GrpcWorkerThread, bool}> worker and whether it hung
     */
    private static array $detached = [];

    private readonly SleeperHandler $sleeper;
    private readonly AsyncPool $asyncPool;
    private ?SleeperHandlerEntry $sleeperEntry = null;
    private ?TaskHandler $task = null;
    private readonly RpcDispatchQueue $queue;
    /** @var ThreadSafeArray<string, mixed> */
    private readonly ThreadSafeArray $gate;
    private readonly int $gateOwner;
    /** @var array<int, GrpcWorkerThread> */
    private array $workers = [];
    /** @var array<int, RpcClient> */
    private array $clients = [];
    private int $nextEndpointId = 1;
    private int $nextWorkerId = 0;
    private int $pendingSpawns;
    private int $nextSpawnNs = 0;
    private int $lastRespawnNs = PHP_INT_MIN >> 1;
    private ?Throwable $spawnError = null;
    private int $outstanding = 0;
    private int $outstandingBytes = 0;
    private int $lostWorkers = 0;
    private int $ticks = 0;
    private bool $closed = false;
    private bool $manageAsync = true;
    private int $nextKeepAsyncNs = 0;

    public function __construct(
        PluginBase $plugin,
        public readonly int $size,
        public readonly int $maxOutstanding,
        public readonly int $maxQueuedBytes
    ){
        if($size < 1 || $size > self::MAX_WORKERS){
            throw new InvalidArgumentException('A worker pool needs between 1 and ' . self::MAX_WORKERS . ' workers.');
        }
        if($maxOutstanding < 1 || $maxQueuedBytes < 1){
            throw new InvalidArgumentException('Pool limits must be greater than 0.');
        }

        $this->sleeper = $plugin->getServer()->getTickSleeper();
        $this->asyncPool = $plugin->getServer()->getAsyncPool();
        $this->queue = new RpcDispatchQueue();
        $this->gate = GrpcGate::get();
        $this->gateOwner = GrpcGate::newKey($this->gate);
        $this->pendingSpawns = $size;
        try{
            $this->sleeperEntry = $this->sleeper->addNotifier(function() : void{
                $this->drainResults();
            });
            // calls from another pool can hold this back, past the wait the tick task finishes it
            $waitUntil = hrtime(true) + self::START_WAIT_NS;
            while(!$this->spawnPending() && hrtime(true) < $waitUntil && $this->spawnError === null){
                usleep(1_000);
            }
            if($this->workers === [] && $this->spawnError !== null){
                throw $this->spawnError;
            }
            $this->task = $plugin->getScheduler()->scheduleRepeatingTask(new ClosureTask(function() : void{
                $this->pump(++$this->ticks % self::WATCHDOG_EVERY_TICKS === 0);
            }), 1);
        }catch(Throwable $e){
            $this->closed = true;
            GrpcGate::resume($this->gate, $this->gateOwner, false);
            $this->queue->stop();
            foreach($this->workers as $worker){
                try{
                    $worker->quit();
                }catch(Throwable){
                }
                GrpcGate::forget($this->gate, $worker->gateKey);
            }
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

    public function isFull(int $bytes) : ?string{
        if($this->outstanding >= $this->maxOutstanding){
            return 'request';
        }
        if($this->outstanding > 0 && $this->outstandingBytes + $bytes > $this->maxQueuedBytes){
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

    /** False once every worker is lost and none is coming, calls would only queue until they time out. */
    public function canServe() : bool{
        return $this->workers !== [] || $this->pendingSpawns > 0;
    }

    public function lostWorkers() : int{
        return $this->lostWorkers;
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
        if($watchdog){
            if($this->outstanding > 0){
                $this->watchdog($now);
            }
            if(self::$detached !== []){
                self::reapDetached();
            }
            if($now >= $this->nextKeepAsyncNs){
                $this->nextKeepAsyncNs = $now + self::KEEP_ASYNC_EVERY_NS;
                $this->keepAsyncWorkers();
            }
        }
        if($this->pendingSpawns > 0 || $this->asyncMissing()){
            $this->spawnPending();
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
            ++$this->lostWorkers;

            if($failed){
                GrpcGate::forget($this->gate, $worker->gateKey);
                $lastError = $worker->getLastError();
                $reason = "gRPC worker $workerId terminated unexpectedly." . ($lastError !== '' ? ' Last error: ' . $lastError : '');
            }else{
                $worker->markHung();
                GrpcGate::lose($this->gate, $worker->gateKey);
                $reason = "gRPC worker $workerId stopped responding inside a call and was replaced.";
            }
            self::$detached[] = [$worker, $stuck];
            \GlobalLogger::get()->warning('pRPC: ' . $reason);
            if($snapshot['endpoint'] >= 0){
                ($this->clients[$snapshot['endpoint']] ?? null)?->onTransportLost($snapshot['ids'], $reason);
            }

            if(self::hungThreads() < self::MAX_HUNG_THREADS){
                // the first replacement is immediate, a crash loop gets one per second
                ++$this->pendingSpawns;
                $this->nextSpawnNs = max($now, $this->lastRespawnNs + self::RESPAWN_EVERY_NS);
                $this->lastRespawnNs = $this->nextSpawnNs;
            }else{
                \GlobalLogger::get()->error('pRPC: ' . self::MAX_HUNG_THREADS . ' worker threads are stuck inside ext-grpc, lost workers are no longer replaced.');
            }
        }
    }

    /**
     * Starts the workers still owed once GrpcGate says it's safe, since a new thread replaces ext-grpc's completion
     * queue under any call that isn't waiting on it yet.
     */
    private function spawnPending() : bool{
        if($this->pendingSpawns === 0 && !$this->asyncMissing()){
            return true;
        }
        if($this->closed || hrtime(true) < $this->nextSpawnNs || !GrpcGate::trySpawn($this->gate, $this->gateOwner)){
            return false;
        }
        $spawned = false;
        try{
            while($this->pendingSpawns > 0){
                $this->spawn();
                --$this->pendingSpawns;
                $spawned = true;
            }
            $spawned = $this->startAsyncWorkers() || $spawned;
        }catch(Throwable $e){
            $this->spawnError = $e;
            \GlobalLogger::get()->logException($e);
            $this->nextSpawnNs = hrtime(true) + self::RESPAWN_EVERY_NS;
        }finally{
            GrpcGate::resume($this->gate, $this->gateOwner, $spawned);
        }
        return $this->pendingSpawns === 0;
    }

    private function asyncMissing() : bool{
        return $this->manageAsync && count($this->asyncPool->getRunningWorkers()) < $this->asyncPool->getSize();
    }

    /** Started now so PocketMine doesn't start them on demand while calls are in flight. */
    private function startAsyncWorkers() : bool{
        $started = false;
        try{
            for($id = 0, $size = $this->asyncPool->getSize(); $id < $size; ++$id){
                if(!in_array($id, $this->asyncPool->getRunningWorkers(), true)){
                    $this->asyncPool->submitTaskToWorker(new RpcKeepAliveTask(), $id);
                    $started = true;
                }
            }
        }catch(Throwable $e){
            $this->manageAsync = false;
            \GlobalLogger::get()->logException($e);
        }
        return $started;
    }

    private function keepAsyncWorkers() : void{
        foreach($this->manageAsync ? $this->asyncPool->getRunningWorkers() : [] as $id){
            try{
                $this->asyncPool->submitTaskToWorker(new RpcKeepAliveTask(), $id);
            }catch(Throwable $e){
                $this->manageAsync = false;
                \GlobalLogger::get()->logException($e);
            }
        }
    }

    private function spawn() : void{
        $worker = new GrpcWorkerThread(
            $this->queue,
            $this->sleeperEntry ?? throw new RuntimeException('No sleeper entry.'),
            $this->gate,
            GrpcGate::newKey($this->gate),
            $this->nextWorkerId++
        );
        $this->queue->addWorker();
        if(!$worker->start()){
            $this->queue->abandon($worker);
            throw new RuntimeException("Failed to start gRPC worker {$worker->workerId}.");
        }
        $this->workers[$worker->workerId] = $worker;
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
        GrpcGate::resume($this->gate, $this->gateOwner, false);
        $this->pendingSpawns = 0;
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

        // let every idle worker leave its loop before joining any, so none is still using the queue while another
        // tears down, busy ones are left to finish their call in the background
        $idle = [];
        foreach($this->workers as $worker){
            if($worker->snapshot()['endpoint'] >= 0){
                self::$detached[] = [$worker, false];
            }else{
                $idle[] = $worker;
            }
        }
        $stopBy = hrtime(true) + self::STOP_WAIT_NS;
        foreach($idle as $worker){
            while($worker->isRunning() && hrtime(true) < $stopBy){
                usleep(200);
            }
        }
        foreach($idle as $worker){
            if($worker->isRunning()){
                self::$detached[] = [$worker, false];
                continue;
            }
            self::join($worker);
        }
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

    private static function hungThreads() : int{
        $n = 0;
        foreach(self::$detached as [, $hung]){
            $n += $hung ? 1 : 0;
        }
        return $n;
    }

    private static function join(GrpcWorkerThread $worker) : void{
        try{
            $worker->quit();
        }catch(Throwable){
        }
        GrpcGate::forget(GrpcGate::get(), $worker->gateKey);
    }

    private static function reapDetached() : void{
        foreach(self::$detached as $i => [$worker]){
            if(!$worker->isRunning()){
                self::join($worker);
                unset(self::$detached[$i]);
            }
        }
    }
}
