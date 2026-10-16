<?php

declare(strict_types=1);

namespace Trix\pRPC\retry;

use Closure;
use Throwable;
use Trix\pRPC\utils\exception\RpcClosedException;
use Trix\pRPC\utils\promise\RpcPromise;
use Trix\pRPC\utils\promise\RpcPromiseResolver;

/** @internal */
final class RpcRetrier{
    /** @var array<int, array{dueNs: int, run: Closure(): void}> */
    private array $timers = [];
    private int $nextTimer = 0;
    private int $retries = 0;
    private bool $stopped = false;

    /**
     * @param Closure(Throwable): void $callbackErrorHandler
     * @param Closure(): int $clockNs
     */
    public function __construct(
        private readonly Closure $callbackErrorHandler,
        private readonly Closure $clockNs
    ){}

    /**
     * @template T
     * @param Closure(): RpcPromise<T> $attempt
     * @param ?Closure(Throwable, int): void $onRetry
     * @return RpcPromise<T>
     */
    public function run(Closure $attempt, RpcRetryPolicy $policy, ?Closure $onRetry = null) : RpcPromise{
        $resolver = new RpcPromiseResolver($this->callbackErrorHandler);
        $giveUpNs = $policy->totalTimeoutMs === null ? PHP_INT_MAX : ($this->clockNs)() + $policy->totalTimeoutMs * 1_000_000;
        $this->attempt($attempt, $policy, $onRetry, $resolver, 1, 0, $giveUpNs);
        return $resolver->promise();
    }

    /** @param RpcPromiseResolver<mixed> $resolver */
    private function attempt(Closure $attempt, RpcRetryPolicy $policy, ?Closure $onRetry, RpcPromiseResolver $resolver, int $n, int $uncertain, int $giveUpNs) : void{
        try{
            $promise = $attempt();
        }catch(Throwable $e){
            $resolver->reject($e);
            return;
        }
        $promise
            ->then(static fn(mixed $value) => $resolver->resolve($value))
            ->onError(function(Throwable $e) use ($attempt, $policy, $onRetry, $resolver, $n, $uncertain, $giveUpNs) : void{
                $ambiguous = RpcStatus::isAmbiguous($e);
                // usually final, so these only get a few extra tries
                $uncertainRetry = $ambiguous && !RpcStatus::isRetryable($e) && !$e instanceof RpcClosedException;
                if($uncertainRetry){
                    ++$uncertain;
                }
                $retry = !$this->stopped
                    && $n < $policy->maxAttempts
                    && ($policy->retryAmbiguous || !$ambiguous)
                    && ($uncertainRetry ? $uncertain < $policy->maxUncertainAttempts : RpcStatus::isRetryable($e));
                $dueNs = $retry ? ($this->clockNs)() + $policy->delayMs($n) * 1_000_000 : 0;
                if(!$retry || $dueNs > $giveUpNs){
                    $resolver->reject($e);
                    return;
                }
                ++$this->retries;
                if($onRetry !== null){
                    try{
                        $onRetry($e, $n);
                    }catch(Throwable $hookError){
                        ($this->callbackErrorHandler)($hookError);
                    }
                }
                $this->timers[$this->nextTimer++] = [
                    'dueNs' => $dueNs,
                    'run' => fn() => $this->attempt($attempt, $policy, $onRetry, $resolver, $n + 1, $uncertain, $giveUpNs),
                ];
            });
    }

    public function tick() : void{
        if($this->timers === []){
            return;
        }
        $now = ($this->clockNs)();
        foreach($this->timers as $id => $timer){
            if($timer['dueNs'] > $now){
                continue;
            }
            unset($this->timers[$id]);
            ($timer['run'])();
        }
    }

    public function waiting() : int{
        return count($this->timers);
    }

    public function retries() : int{
        return $this->retries;
    }

    public function stop() : void{
        $this->stopped = true;
        $timers = $this->timers;
        $this->timers = [];
        foreach($timers as $timer){
            ($timer['run'])();
        }
    }
}
