<?php

declare(strict_types=1);

namespace Trix\pRPC\thread;

use pmmp\thread\ThreadSafe;
use pmmp\thread\ThreadSafeArray;

/**
 * One endpoint's queue inside the shared dispatch queue. Only touched while holding the queue's lock.
 *
 * @internal
 */
final class RpcLane extends ThreadSafe{
    /** @var ThreadSafeArray<int, string> */
    public ThreadSafeArray $jobs;
    public int $busy = 0;
    public int $connectivity = GrpcWorkerThread::CONNECTIVITY_UNKNOWN;
    public bool $probing = false;
    public int $reconnects = 0;

    public function __construct(
        public readonly string $config,
        public readonly int $cap,
        public readonly int $batchSize
    ){
        $this->jobs = new ThreadSafeArray();
    }
}
