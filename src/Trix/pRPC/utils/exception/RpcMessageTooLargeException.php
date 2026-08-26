<?php

declare(strict_types=1);

namespace Trix\pRPC\utils\exception;

use const Grpc\STATUS_RESOURCE_EXHAUSTED;

final class RpcMessageTooLargeException extends RpcException{
    public function __construct(string $direction, int $size, int $limit){
        parent::__construct("RPC $direction is $size bytes, exceeding the $limit-byte limit.", STATUS_RESOURCE_EXHAUSTED);
    }
}
