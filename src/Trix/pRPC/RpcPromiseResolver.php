<?php

declare(strict_types=1);

namespace Trix\pRPC;

use Throwable;

/**
 * @internal
 * @template TValue
 */
final class RpcPromiseResolver{
    /** @var RpcPromise<TValue> */
    private RpcPromise $promise;

    public function __construct(){
        $this->promise = new RpcPromise();
    }

    /** @return RpcPromise<TValue> */
    public function promise() : RpcPromise{
        return $this->promise;
    }

    /** @phpstan-param TValue $value */
    public function resolve(mixed $value) : void{
        $this->promise->resolve($value);
    }

    public function reject(Throwable $error) : void{
        $this->promise->reject($error);
    }
}
