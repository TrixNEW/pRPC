<?php

declare(strict_types=1);

namespace Trix\pRPC\utils\promise;

use Closure;
use Throwable;

/**
 * @internal
 * @template TValue
 */
final class RpcPromiseResolver{
    /** @var RpcPromise<TValue> */
    private RpcPromise $promise;

    /** @param Closure(Throwable): void $callbackErrorHandler */
    public function __construct(Closure $callbackErrorHandler){
        $this->promise = new RpcPromise($callbackErrorHandler);
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
