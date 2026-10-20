<?php

declare(strict_types=1);

namespace Grpc;

/** Microseconds on the hrtime clock, unlike ext-grpc's realtime one. */
class Timeval{
    public function __construct(public readonly int $us){}

    public static function now() : self{
        return new self(intdiv(hrtime(true), 1_000));
    }

    public function add(Timeval $other) : self{
        return new self($this->us + $other->us);
    }
}
