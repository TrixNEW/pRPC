<?php

declare(strict_types=1);

namespace Trix\pRPC\exception;

use const Grpc\STATUS_RESOURCE_EXHAUSTED;

final class RpcOverloadedException extends RpcException{
    public function __construct(int $maxPending){
        parent::__construct("RPC client has reached its $maxPending-request pending limit.", STATUS_RESOURCE_EXHAUSTED);
    }
}
