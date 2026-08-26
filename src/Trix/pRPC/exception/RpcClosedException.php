<?php

declare(strict_types=1);

namespace Trix\pRPC\exception;

use const Grpc\STATUS_CANCELLED;

final class RpcClosedException extends RpcException{
    public function __construct(string $message = 'RPC client is closed.'){
        parent::__construct($message, STATUS_CANCELLED);
    }
}
