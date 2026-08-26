<?php

declare(strict_types=1);

namespace Trix\pRPC\utils\exception;

use const Grpc\STATUS_DEADLINE_EXCEEDED;

final class RpcTimeoutException extends RpcException{
    public function __construct(int $timeoutMs){
        parent::__construct("RPC timed out after $timeoutMs ms.", STATUS_DEADLINE_EXCEEDED);
    }
}
