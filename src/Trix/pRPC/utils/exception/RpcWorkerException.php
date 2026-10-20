<?php

declare(strict_types=1);

namespace Trix\pRPC\utils\exception;

use const Grpc\STATUS_INTERNAL;

final class RpcWorkerException extends RpcException{
    /** @param bool $sent false when the request never reached a worker, so retrying it is safe */
    public function __construct(string $message, public readonly bool $sent = true){
        parent::__construct($message, STATUS_INTERNAL);
    }
}
