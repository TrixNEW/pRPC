<?php

declare(strict_types=1);

namespace Trix\pRPC\stats;

/**
 * A point in time view of one client. Worker fields describe the pool it runs on, which a manager shares between
 * clients. Counters are totals since the client was created.
 */
final readonly class RpcClientStats{
    /**
     * @param list<int> $workerLoads calls in each worker's current batch
     * @param list<string> $workerStates
     * @param string $connectivity channel state last seen by a worker: idle, connecting, ready, transient_failure, shutdown or unknown
     * @param int $queued requests waiting for a worker
     * @param int $activeWorkers workers running a batch for this client right now
     * @param int $lostWorkers workers the pool replaced after they died or hung
     * @param int $timeouts local timeouts plus DEADLINE_EXCEEDED
     * @param int $overloaded calls rejected by an outstanding or queued bytes limit
     * @param int $transportErrors UNAVAILABLE and lost worker failures
     * @param int $reconnects times a worker saw the channel come back after it was not ready
     * @param ?RpcLatency $latency call to callback time of answered calls
     * @param ?RpcLatency $queueWait time from call() until a worker started the request
     */
    public function __construct(
        public int $pending,
        public int $transportOutstanding,
        public array $workerLoads,
        public array $workerStates,
        public int $retrying = 0,
        public string $endpoint = '',
        public string $connectivity = 'unknown',
        public int $queued = 0,
        public int $activeWorkers = 0,
        public int $queuedBytes = 0,
        public int $threads = 0,
        public int $lostWorkers = 0,
        public int $completed = 0,
        public int $failed = 0,
        public int $timeouts = 0,
        public int $overloaded = 0,
        public int $transportErrors = 0,
        public int $retries = 0,
        public int $reconnects = 0,
        public ?RpcLatency $latency = null,
        public ?RpcLatency $queueWait = null
    ){}
}
