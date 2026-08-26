<?php

declare(strict_types=1);

namespace Trix\pRPC\utils\exception;

use RuntimeException;
use const Grpc\STATUS_UNKNOWN;

class RpcException extends RuntimeException{
    /** @param array<string, mixed> $metadata */
    public function __construct(
        string $message,
        private readonly int $statusCode = STATUS_UNKNOWN,
        private readonly array $metadata = []
    ){
        parent::__construct($message, $statusCode);
    }

    public function getStatusCode() : int{
        return $this->statusCode;
    }

    /** @return array<string, mixed> */
    public function getMetadata() : array{
        return $this->metadata;
    }
}
