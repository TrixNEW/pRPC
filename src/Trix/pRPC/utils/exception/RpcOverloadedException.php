<?php

declare(strict_types=1);

namespace Trix\pRPC\utils\exception;

use const Grpc\STATUS_RESOURCE_EXHAUSTED;

final class RpcOverloadedException extends RpcException{
    public function __construct(int $maxOutstanding){
        parent::__construct("RPC transport has reached its {$maxOutstanding}-request outstanding limit.", STATUS_RESOURCE_EXHAUSTED);
    }
}
