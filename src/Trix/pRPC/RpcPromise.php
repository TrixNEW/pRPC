<?php

declare(strict_types=1);

namespace Trix\pRPC;

use Closure;
use LogicException;
use Throwable;

/**
 * @template TValue
 */
final class RpcPromise{
    private const int PENDING = 0;
    private const int RESOLVED = 1;
    private const int REJECTED = 2;

    private int $state = self::PENDING;
    private mixed $value = null;
    private ?Throwable $error = null;

    /** @var array<int, Closure(TValue): void> */
    private array $successCallbacks = [];

    /** @var array<int, Closure(Throwable): void> */
    private array $errorCallbacks = [];

    /** @var array<int, Closure(): void> */
    private array $alwaysCallbacks = [];

    /** @phpstan-param Closure(TValue): void $callback */
    public function then(Closure $callback) : self{
        if($this->state === self::RESOLVED){
            $callback($this->value);
        }elseif($this->state === self::PENDING){
            $this->successCallbacks[spl_object_id($callback)] = $callback;
        }
        return $this;
    }

    /** @param Closure(Throwable): void $callback */
    public function onError(Closure $callback) : self{
        if($this->state === self::REJECTED){
            $callback($this->error);
        }elseif($this->state === self::PENDING){
            $this->errorCallbacks[spl_object_id($callback)] = $callback;
        }
        return $this;
    }

    /** @param Closure(): void $callback */
    public function always(Closure $callback) : self{
        if($this->state === self::PENDING){
            $this->alwaysCallbacks[spl_object_id($callback)] = $callback;
        }else{
            $callback();
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

        try{
            foreach($success as $callback){
                $callback($value);
            }
        }finally{
            foreach($always as $callback){
                $callback();
            }
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

        try{
            foreach($errors as $callback){
                $callback($error);
            }
        }finally{
            foreach($always as $callback){
                $callback();
            }
        }
    }

    private function clearCallbacks() : void{
        $this->successCallbacks = [];
        $this->errorCallbacks = [];
        $this->alwaysCallbacks = [];
    }
}
