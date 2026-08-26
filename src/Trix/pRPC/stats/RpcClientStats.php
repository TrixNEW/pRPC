<?php

declare(strict_types=1);

namespace Trix\pRPC\stats;

final readonly class RpcClientStats{
    /**
     * @param list<int> $workerLoads
     * @param list<string> $workerStates
     */
    public function __construct(
        public int $pending,
        public int $transportOutstanding,
        public array $workerLoads,
        public array $workerStates
    ){}
}
