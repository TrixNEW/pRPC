<?php

declare(strict_types=1);

namespace Trix\pRPC;

use Closure;
use LogicException;
use Throwable;

/**
 * main-thread promise for RPC completion.
 *
 * @template TValue
 */
final class RpcPromise{
    private const int PENDING = 0;
    private const int RESOLVED = 1;
    private const int REJECTED = 2;

    private int $state = self::PENDING;
    private mixed $value = null;
    private ?Throwable $error = null;

    /** @var list<Closure(TValue): void> */
    private array $successCallbacks = [];

    /** @var list<Closure(Throwable): void> */
    private array $errorCallbacks = [];

    /** @var list<Closure(): void> */
    private array $alwaysCallbacks = [];

    /** @param Closure(Throwable): void $callbackErrorHandler */
    public function __construct(private readonly Closure $callbackErrorHandler){}

    /** @phpstan-param Closure(TValue): void $callback */
    public function then(Closure $callback) : self{
        if($this->state === self::RESOLVED){
            $this->invoke($callback, $this->value);
        }elseif($this->state === self::PENDING){
            $this->successCallbacks[] = $callback;
        }
        return $this;
    }

    /** @param Closure(Throwable): void $callback */
    public function onError(Closure $callback) : self{
        if($this->state === self::REJECTED){
            $this->invoke($callback, $this->error);
        }elseif($this->state === self::PENDING){
            $this->errorCallbacks[] = $callback;
        }
        return $this;
    }

    /** @param Closure(): void $callback */
    public function always(Closure $callback) : self{
        if($this->state === self::PENDING){
            $this->alwaysCallbacks[] = $callback;
        }else{
            $this->invoke($callback);
        }
        return $this;
    }

    public function isPending() : bool{
        return $this->state === self::PENDING;
    }

    public function isResolved() : bool{
        return $this->state === self::RESOLVED;
    }

    public function isRejected() : bool{
        return $this->state === self::REJECTED;
    }

    /** @internal */
    public function resolve(mixed $value) : void{
        if($this->state !== self::PENDING){
            throw new LogicException('RPC promise has already settled.');
        }

        $this->state = self::RESOLVED;
        $this->value = $value;

        $success = $this->successCallbacks;
        $always = $this->alwaysCallbacks;
        $this->clearCallbacks();

        foreach($success as $callback){
            $this->invoke($callback, $value);
        }
        foreach($always as $callback){
            $this->invoke($callback);
        }
    }

    /** @internal */
    public function reject(Throwable $error) : void{
        if($this->state !== self::PENDING){
            throw new LogicException('RPC promise has already settled.');
        }

        $this->state = self::REJECTED;
        $this->error = $error;

        $errors = $this->errorCallbacks;
        $always = $this->alwaysCallbacks;
        $this->clearCallbacks();

        foreach($errors as $callback){
            $this->invoke($callback, $error);
        }
        foreach($always as $callback){
            $this->invoke($callback);
        }
    }

    private function invoke(Closure $callback, mixed ...$args) : void{
        try{
            $callback(...$args);
        }catch(Throwable $e){
            try{
                ($this->callbackErrorHandler)($e);
            }catch(Throwable){
            }
        }
    }

    private function clearCallbacks() : void{
        $this->successCallbacks = [];
        $this->errorCallbacks = [];
        $this->alwaysCallbacks = [];
    }
}
