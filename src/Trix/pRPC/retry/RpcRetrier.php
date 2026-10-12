<?php

declare(strict_types=1);

namespace Trix\pRPC\retry;

use Closure;
use Throwable;
use Trix\pRPC\utils\exception\RpcClosedException;
use Trix\pRPC\utils\promise\RpcPromise;
use Trix\pRPC\utils\promise\RpcPromiseResolver;

/**
 * Runs attempts until one succeeds, a failure is final or the policy gives up.
 * Delays are kept in a local timer list that the owner advances from the main thread.
 *
 * @internal
 */
final class RpcRetrier{
    /** @var array<int, array{dueNs: int, run: Closure(): void}> */
    private array $timers = [];
    private int $nextTimer = 0;
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
     * @param ?Closure(Throwable, int): void $onRetry called before each retry with the failure and attempt number
     * @return RpcPromise<T>
     */
    public function run(Closure $attempt, RpcRetryPolicy $policy, ?Closure $onRetry = null) : RpcPromise{
        $resolver = new RpcPromiseResolver($this->callbackErrorHandler);
        $this->attempt($attempt, $policy, $onRetry, $resolver, 1, 0);
        return $resolver->promise();
    }

    /** @param RpcPromiseResolver<mixed> $resolver */
    private function attempt(Closure $attempt, RpcRetryPolicy $policy, ?Closure $onRetry, RpcPromiseResolver $resolver, int $n, int $uncertain) : void{
        try{
            $promise = $attempt();
        }catch(Throwable $e){
            $resolver->reject($e);
            return;
        }
        $promise
            ->then(static fn(mixed $value) => $resolver->resolve($value))
            ->onError(function(Throwable $e) use ($attempt, $policy, $onRetry, $resolver, $n, $uncertain) : void{
                $ambiguous = RpcStatus::isAmbiguous($e);
                // ambiguous but usually final, like a reset stream, so a deduplicated request gets a few more tries
                $uncertainRetry = $ambiguous && !RpcStatus::isRetryable($e) && !$e instanceof RpcClosedException;
                if($uncertainRetry){
                    ++$uncertain;
                }
                $retry = !$this->stopped
                    && $n < $policy->maxAttempts
                    && ($policy->retryAmbiguous || !$ambiguous)
                    && ($uncertainRetry ? $uncertain < $policy->maxUncertainAttempts : RpcStatus::isRetryable($e));
                if(!$retry){
                    $resolver->reject($e);
                    return;
                }
                if($onRetry !== null){
                    try{
                        $onRetry($e, $n);
                    }catch(Throwable $hookError){
                        ($this->callbackErrorHandler)($hookError);
                    }
                }
                $this->timers[$this->nextTimer++] = [
                    'dueNs' => ($this->clockNs)() + $policy->delayMs($n) * 1_000_000,
                    'run' => fn() => $this->attempt($attempt, $policy, $onRetry, $resolver, $n + 1, $uncertain),
                ];
            });
    }

    /** Starts every retry whose delay has passed. */
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

    /** Runs the waiting retries now so they can settle, and stops scheduling new ones. */
    public function stop() : void{
        $this->stopped = true;
        $timers = $this->timers;
        $this->timers = [];
        foreach($timers as $timer){
            ($timer['run'])();
        }
    }
}
