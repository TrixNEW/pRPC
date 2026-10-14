<?php

declare(strict_types=1);

namespace Trix\pRPC\utils\exception;

use const Grpc\STATUS_RESOURCE_EXHAUSTED;

final class RpcOverloadedException extends RpcException{
    /** @param string $unit 'request' for the outstanding limit, 'byte' for the queued bytes limit */
    public function __construct(int $limit, string $unit = 'request'){
        parent::__construct($unit === 'byte'
            ? "RPC transport has reached its {$limit}-byte queued limit."
            : "RPC transport has reached its {$limit}-request outstanding limit.", STATUS_RESOURCE_EXHAUSTED);
    }
}
