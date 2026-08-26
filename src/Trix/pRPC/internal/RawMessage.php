<?php

declare(strict_types=1);

namespace Trix\pRPC\internal;

/**
 * Minimal stood message so protobuf encode/decode never touches the worker thread.
 * BaseStub only needs serializeToString()/mergeFromString() for unary calls, so that's all this implements.
 *
 * @internal
 */
final class RawMessage{
    public function __construct(private string $bytes = ''){}

    public function serializeToString() : string{
        return $this->bytes;
    }

    public function mergeFromString(string $bytes) : void{
        $this->bytes = $bytes;
    }

    public function bytes() : string{
        return $this->bytes;
    }

    public static function decode(string $bytes) : self{
        return new self($bytes);
    }
}
