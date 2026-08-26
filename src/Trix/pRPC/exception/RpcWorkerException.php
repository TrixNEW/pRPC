<?php

declare(strict_types=1);

namespace Trix\pRPC\exception;

use const Grpc\STATUS_INTERNAL;

final class RpcWorkerException extends RpcException{
    public function __construct(string $message){
        parent::__construct($message, STATUS_INTERNAL);
    }
}
