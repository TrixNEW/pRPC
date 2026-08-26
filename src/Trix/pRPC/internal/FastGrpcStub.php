<?php

declare(strict_types=1);

namespace Trix\pRPC\internal;

use Grpc\BaseStub;
use Grpc\UnaryCall;
use RuntimeException;

/** @internal */
final class FastGrpcStub extends BaseStub{
    /**
     * Starts a unary request using already serialized protobuf bytes.
     * This bypasses generated request/response object construction in the worker.
     *
     * @param array<string, list<string>> $metadata
     * @param array<string, mixed> $options
     */
    public function rawUnary(string $path, string $requestBytes, array $metadata, array $options) : UnaryCall{
        $call = $this->_simpleRequest(
            $path,
            new RawMessage($requestBytes),
            [RawMessage::class, 'decode'],
            $metadata,
            $options
        );

        if(!$call instanceof UnaryCall){
            throw new RuntimeException("$path did not create a unary gRPC call.");
        }
        return $call;
    }
}
