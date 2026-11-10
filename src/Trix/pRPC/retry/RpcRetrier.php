<?php

declare(strict_types=1);

namespace Trix\pRPC\retry;

use Closure;
use Throwable;
use Trix\pRPC\utils\exception\RpcClosedException;
use Vow\Deferred;
use Vow\Promise;

/** @internal */
final class RpcRetrier{
    /** @var array<int, array{Deferred<mixed>, ?Promise<mixed>}> run => its deferred and the attempt in flight */
    private array $runs = [];
    /** @var array<int, array{dueNs: int, run: Closure(): void}> run => its next attempt */
    private array $timers = [];
    private int $nextRun = 0;
    private int $retries = 0;
    private bool $stopped = false;

    /**
     * @param Closure(Throwable): void $callbackErrorHandler
     * @param Closure(): int $clockNs
     * @param Closure(Promise<mixed>): bool $cancelAttempt
     */
    public function __construct(
        private readonly Closure $callbackErrorHandler,
        private readonly Closure $clockNs,
        private readonly Closure $cancelAttempt
    ){}

    /**
     * @template T
     * @param Closure(): Promise<T> $attempt
     * @param ?Closure(Throwable, int): void $onRetry
     * @return Promise<T>
     */
    public function run(Closure $attempt, RpcRetryPolicy $policy, ?Closure $onRetry = null) : Promise{
        /** @var Deferred<T> $deferred */
        $deferred = new Deferred();
        $run = $this->nextRun++;
        $this->runs[$run] = [$deferred, null];
        $giveUpNs = $policy->totalTimeoutMs === null ? PHP_INT_MAX : ($this->clockNs)() + $policy->totalTimeoutMs * 1_000_000;
        $this->attempt($run, $attempt, $policy, $onRetry, 1, 0, $giveUpNs);
        return $deferred->promise;
    }

    private function attempt(int $run, Closure $attempt, RpcRetryPolicy $policy, ?Closure $onRetry, int $n, int $uncertain, int $giveUpNs) : void{
        // a callback can cancel a run while its timer is already being fired
        if(!isset($this->runs[$run])){
            return;
        }
        try{
            $promise = $attempt();
        }catch(Throwable $e){
            $this->finish($run, false, $e);
            return;
        }
        $this->runs[$run][1] = $promise;
        $promise->observe(
            fn(mixed $value) => $this->finish($run, true, $value),
            function(Throwable $e) use ($run, $attempt, $policy, $onRetry, $n, $uncertain, $giveUpNs) : void{
                if(!isset($this->runs[$run])){
                    return;
                }
                $this->runs[$run][1] = null;
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
                    $this->finish($run, false, $e);
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
                $this->timers[$run] = [
                    'dueNs' => $dueNs,
                    'run' => fn() => $this->attempt($run, $attempt, $policy, $onRetry, $n + 1, $uncertain, $giveUpNs),
                ];
            }
        );
    }

    private function finish(int $run, bool $fulfilled, mixed $result) : void{
        $deferred = ($this->runs[$run] ?? null)[0] ?? null;
        if($deferred === null){
            return;
        }
        unset($this->runs[$run]);
        try{
            $fulfilled ? $deferred->resolve($result) : $deferred->reject($result);
        }catch(Throwable $e){
            ($this->callbackErrorHandler)($e);
        }
    }

    /** @param Promise<mixed> $promise */
    public function cancel(Promise $promise) : bool{
        foreach($this->runs as $run => [$deferred, $inFlight]){
            if($deferred->promise !== $promise){
                continue;
            }
            unset($this->runs[$run], $this->timers[$run]);
            try{
                if($inFlight !== null){
                    ($this->cancelAttempt)($inFlight);
                }
                $deferred->cancel();
            }catch(Throwable $e){
                ($this->callbackErrorHandler)($e);
            }
            return true;
        }
        return false;
    }

    public function tick() : void{
        if($this->timers === []){
            return;
        }
        $now = ($this->clockNs)();
        foreach($this->timers as $run => $timer){
            if($timer['dueNs'] > $now){
                continue;
            }
            unset($this->timers[$run]);
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
