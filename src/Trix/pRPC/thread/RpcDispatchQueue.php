<?php

declare(strict_types=1);

namespace Trix\pRPC\thread;

use pmmp\thread\ThreadSafe;
use pmmp\thread\ThreadSafeArray;

/**
 * Shared by the main thread and every worker of a pool. Workers pull a batch from one endpoint at a time,
 * so a call never waits behind a busy worker and a slow endpoint can only hold its lane's cap of workers.
 * The last idle worker is kept for endpoints that have none yet, so every endpoint gets a worker while fewer
 * endpoints are busy than there are workers.
 *
 * @internal
 */
final class RpcDispatchQueue extends ThreadSafe{
    /** @var ThreadSafeArray<int, RpcLane> */
    private ThreadSafeArray $lanes;
    /** @var ThreadSafeArray<int, bool> */
    private ThreadSafeArray $abandoned;
    /** @var ThreadSafeArray<int, string> */
    public readonly ThreadSafeArray $results;
    private int $cursor = 0;
    private int $workers = 0;
    private int $busy = 0;
    private bool $stopping = false;
    private int $epoch = 0;

    public function __construct(){
        $this->lanes = new ThreadSafeArray();
        $this->abandoned = new ThreadSafeArray();
        $this->results = new ThreadSafeArray();
    }

    public function addWorker() : void{
        $this->synchronized(function() : void{
            ++$this->workers;
        });
    }

    public function addLane(int $endpointId, RpcLane $lane) : void{
        $this->synchronized(function() use ($endpointId, $lane) : void{
            $this->lanes[$endpointId] = $lane;
            ++$this->epoch;
        });
    }

    /** @return list<string> the jobs that never reached a worker */
    public function removeLane(int $endpointId) : array{
        return $this->synchronized(function() use ($endpointId) : array{
            $lane = $this->lanes[$endpointId] ?? null;
            if($lane === null){
                return [];
            }
            unset($this->lanes[$endpointId]);
            ++$this->epoch;
            return $lane->jobs->chunk(PHP_INT_MAX);
        });
    }

    public function push(int $endpointId, string $job) : bool{
        return $this->synchronized(function() use ($endpointId, $job) : bool{
            $lane = $this->lanes[$endpointId] ?? null;
            if($lane === null || $this->stopping){
                return false;
            }
            $lane->jobs[] = $job;
            $this->notifyOne();
            return true;
        });
    }

    /** Queues a connectivity check unless one is already waiting. */
    public function probe(int $endpointId) : void{
        $this->synchronized(function() use ($endpointId) : void{
            $lane = $this->lanes[$endpointId] ?? null;
            if($lane !== null && !$lane->probing && !$this->stopping){
                $lane->probing = true;
                $lane->jobs[] = GrpcWorkerThread::PROBE_JOB;
                $this->notifyOne();
            }
        });
    }

    public function connectivity(int $endpointId) : int{
        return $this->synchronized(fn() : int => ($this->lanes[$endpointId] ?? null)?->connectivity ?? GrpcWorkerThread::CONNECTIVITY_UNKNOWN);
    }

    /** @return array{queued: int, busy: int, connectivity: int, reconnects: int} */
    public function laneStats(int $endpointId) : array{
        return $this->synchronized(function() use ($endpointId) : array{
            $lane = $this->lanes[$endpointId] ?? null;
            return $lane === null
                ? ['queued' => 0, 'busy' => 0, 'connectivity' => GrpcWorkerThread::CONNECTIVITY_UNKNOWN, 'reconnects' => 0]
                : ['queued' => $lane->jobs->count(), 'busy' => $lane->busy, 'connectivity' => $lane->connectivity, 'reconnects' => $lane->reconnects];
        });
    }

    public function epoch() : int{
        return $this->synchronized(fn() : int => $this->epoch);
    }

    public function laneConfig(int $endpointId) : ?string{
        return $this->synchronized(fn() : ?string => ($this->lanes[$endpointId] ?? null)?->config);
    }

    /**
     * Blocks until a batch is available, round robin across endpoints whose lane is under its worker cap.
     *
     * @return array{int, list<string>}|null null once the pool is stopping and nothing is left to run
     */
    public function take(GrpcWorkerThread $worker) : ?array{
        return $this->synchronized(function() use ($worker) : ?array{
            while(true){
                if(isset($this->abandoned[$worker->workerId])){
                    return null;
                }
                $eligible = [];
                $reserve = $this->lanes->count() > 1 && $this->workers - $this->busy < 2;
                foreach($this->lanes as $id => $lane){
                    if($lane->busy < $lane->cap && ($lane->busy === 0 || !$reserve) && $lane->jobs->count() > 0){
                        $eligible[] = $id;
                    }
                }
                if($eligible !== []){
                    $pick = $eligible[0];
                    foreach($eligible as $id){
                        if($id > $this->cursor){
                            $pick = $id;
                            break;
                        }
                    }
                    $lane = $this->lanes[$pick];
                    $jobs = $lane->jobs->chunk($lane->batchSize);
                    ++$lane->busy;
                    ++$this->busy;
                    $this->cursor = $pick;
                    $ids = '';
                    $untilNs = 0;
                    foreach($jobs as $job){
                        if($job !== GrpcWorkerThread::PROBE_JOB){
                            $h = unpack('Jid/Jdeadline', $job);
                            $ids .= pack('J', $h['id']);
                            $untilNs = max($untilNs, $h['deadline']);
                        }
                    }
                    $worker->markBusy($pick, $untilNs, $ids);
                    // hand the rest to another idle worker instead of waking all of them on every push
                    if(count($eligible) > 1 || ($lane->busy < $lane->cap && $lane->jobs->count() > 0)){
                        $this->notifyOne();
                    }
                    return [$pick, $jobs];
                }
                if($this->stopping){
                    return null;
                }
                $this->wait();
            }
        });
    }

    /** @return bool false when the main thread gave up on this worker, which must then exit */
    public function finish(GrpcWorkerThread $worker, int $endpointId, int $connectivity, bool $probed) : bool{
        return $this->synchronized(function() use ($worker, $endpointId, $connectivity, $probed) : bool{
            if(isset($this->abandoned[$worker->workerId])){
                return false;
            }
            $worker->markIdle();
            --$this->busy;
            $lane = $this->lanes[$endpointId] ?? null;
            if($lane !== null){
                --$lane->busy;
                if($connectivity !== GrpcWorkerThread::CONNECTIVITY_UNKNOWN){
                    if($connectivity === GrpcWorkerThread::CONNECTIVITY_READY && $lane->connectivity !== GrpcWorkerThread::CONNECTIVITY_READY && $lane->connectivity !== GrpcWorkerThread::CONNECTIVITY_UNKNOWN){
                        ++$lane->reconnects;
                    }
                    $lane->connectivity = $connectivity;
                }
                if($probed){
                    $lane->probing = false;
                }
            }
            return true;
        });
    }

    /**
     * Frees the lane slot of a worker stuck in a call that will never return.
     *
     * @return bool false if it finished on its own in the meantime
     */
    public function abandon(GrpcWorkerThread $worker) : bool{
        return $this->synchronized(function() use ($worker) : bool{
            $endpointId = $worker->busyEndpoint();
            $this->abandoned[$worker->workerId] = true;
            --$this->workers;
            if($endpointId < 0){
                return false;
            }
            --$this->busy;
            $lane = $this->lanes[$endpointId] ?? null;
            if($lane !== null){
                --$lane->busy;
                $lane->probing = false;
            }
            $this->notifyOne();
            return true;
        });
    }

    public function stop() : void{
        $this->synchronized(function() : void{
            $this->stopping = true;
            $this->notify();
        });
    }

    /** @return array<int, list<string>> queued jobs per endpoint, removed from the queue */
    public function discardQueued() : array{
        return $this->synchronized(function() : array{
            $dropped = [];
            foreach($this->lanes as $id => $lane){
                $jobs = $lane->jobs->chunk(PHP_INT_MAX);
                if($jobs !== []){
                    $dropped[$id] = $jobs;
                }
            }
            return $dropped;
        });
    }
}
