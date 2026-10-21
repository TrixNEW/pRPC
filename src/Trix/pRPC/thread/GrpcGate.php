<?php

declare(strict_types=1);

namespace Trix\pRPC\thread;

use pmmp\thread\ThreadSafeArray;

/**
 * Guards ext-grpc's process wide completion queue. It allows at most 6 waiters, so at most 6 workers may be inside a
 * call across every pool. Starting a thread replaces the queue, and a call created on the old one then waits forever,
 * so pRPC only starts threads while no worker is between creating a call and waiting on it. Threads started by
 * anything else can't be held back. It lives in $GLOBALS so every shaded copy of pRPC in the server shares it.
 * None of this applies to an ext-grpc with a queue per thread, see needed().
 *
 * @internal
 */
final class GrpcGate{
    public const int MAX_CALLS = 6;
    // with a queue per thread the only limit left is how many threads are worth running
    public const int MAX_WORKERS_PER_THREAD_QUEUE = 16;
    private const string KEY = 'pRPC.grpcGate.v1';
    // how long a spawn may hold calls back before it gives up and lets them through for a while
    private const int PAUSE_NS = 3_000_000_000;
    private const int RETRY_NS = 1_000_000_000;
    // a worker that entered without a window is microseconds from waiting, this covers it being descheduled
    private const int SETTLE_NS = 25_000_000;

    private const int IN_CALL = 1;
    // a hung worker may still be waiting on the current queue, so its slot stays taken until the next spawn
    private const int LOST = 2;
    private const int RELEASED = 3;

    /** @return ThreadSafeArray<string, mixed> */
    public static function get() : ThreadSafeArray{
        $gate = $GLOBALS[self::KEY] ?? null;
        if(!$gate instanceof ThreadSafeArray){
            $gate = new ThreadSafeArray();
            $gate['calls'] = 0;
            $gate['lost'] = 0;
            $gate['seq'] = 0;
            $gate['pausedBy'] = 0;
            $gate['pauseUntil'] = 0;
            $gate['quietSince'] = 0;
            $gate['retryAt'] = 0;
            $gate['slots'] = new ThreadSafeArray();
            $gate['windows'] = new ThreadSafeArray();
            $GLOBALS[self::KEY] = $gate;
        }
        return $gate;
    }

    /** False on an ext-grpc built with a completion queue per thread, like github.com/TrixNEW/ext-grpc. */
    public static function needed() : bool{
        return !defined('Grpc\PER_THREAD_COMPLETION_QUEUE');
    }

    public static function maxWorkers() : int{
        return self::needed() ? self::MAX_CALLS : self::MAX_WORKERS_PER_THREAD_QUEUE;
    }

    /** @param ThreadSafeArray<string, mixed> $gate */
    public static function newKey(ThreadSafeArray $gate) : int{
        return $gate->synchronized(static function() use ($gate) : int{
            return $gate['seq'] = $gate['seq'] + 1;
        });
    }

    /**
     * Takes a call slot, waiting while 6 are taken or a spawn is pending. A worker that runs one call per slot is
     * waiting on the queue microseconds later, so only one that creates several calls first needs a window.
     *
     * @param ThreadSafeArray<string, mixed> $gate
     */
    public static function enter(ThreadSafeArray $gate, int $key, bool $window) : void{
        if(!self::needed()){
            return;
        }
        $gate->synchronized(static function() use ($gate, $key, $window) : void{
            while(true){
                $pausedFor = $gate['pausedBy'] === 0 ? 0 : $gate['pauseUntil'] - hrtime(true);
                if($pausedFor <= 0 && $gate['calls'] < self::MAX_CALLS){
                    break;
                }
                $gate->wait($pausedFor > 0 ? max(1, intdiv($pausedFor, 1_000)) : 0);
            }
            $gate['calls'] = $gate['calls'] + 1;
            $gate['slots'][$key] = self::IN_CALL;
            if($window){
                $gate['windows'][$key] = true;
            }
        });
    }

    /** @param ThreadSafeArray<string, mixed> $gate */
    public static function leave(ThreadSafeArray $gate, int $key) : void{
        if(!self::needed()){
            return;
        }
        $gate->synchronized(static function() use ($gate, $key) : void{
            self::free($gate, $key);
        });
    }

    /**
     * Marks a worker that stopped responding inside a call.
     *
     * @param ThreadSafeArray<string, mixed> $gate
     */
    public static function lose(ThreadSafeArray $gate, int $key) : void{
        $gate->synchronized(static function() use ($gate, $key) : void{
            if(($gate['slots'][$key] ?? 0) === self::IN_CALL){
                $gate['slots'][$key] = self::LOST;
                $gate['lost'] = $gate['lost'] + 1;
            }
        });
    }

    /**
     * Frees the slot of a worker whose thread has ended.
     *
     * @param ThreadSafeArray<string, mixed> $gate
     */
    public static function forget(ThreadSafeArray $gate, int $key) : void{
        $gate->synchronized(static function() use ($gate, $key) : void{
            self::free($gate, $key);
        });
    }

    /**
     * Holds new calls back until starting a thread is safe. Call it again until it returns true, then start the
     * threads and call resume().
     *
     * @param ThreadSafeArray<string, mixed> $gate
     */
    public static function trySpawn(ThreadSafeArray $gate, int $owner) : bool{
        return $gate->synchronized(static function() use ($gate, $owner) : bool{
            $now = hrtime(true);
            if($gate['pausedBy'] !== $owner){
                if(($gate['pausedBy'] !== 0 && $now < $gate['pauseUntil']) || $now < $gate['retryAt']){
                    return false;
                }
                $gate['pausedBy'] = $owner;
                $gate['pauseUntil'] = $now + self::PAUSE_NS;
                $gate['quietSince'] = 0;
            }elseif($now >= $gate['pauseUntil']){
                $gate['pausedBy'] = 0;
                $gate['retryAt'] = $now + self::RETRY_NS;
                $gate->notify();
                return false;
            }
            if($gate['calls'] === $gate['lost']){
                return true;
            }
            $slots = $gate['slots'];
            foreach($gate['windows'] as $key => $_){
                if($slots[$key] === self::IN_CALL){
                    $gate['quietSince'] = 0;
                    return false;
                }
            }
            if($gate['quietSince'] === 0){
                $gate['quietSince'] = $now;
            }
            return $now - $gate['quietSince'] >= self::SETTLE_NS;
        });
    }

    /**
     * Ends a pause. After a spawn, hung workers wait on a queue nothing uses anymore, so their slots are given back.
     *
     * @param ThreadSafeArray<string, mixed> $gate
     */
    public static function resume(ThreadSafeArray $gate, int $owner, bool $spawned) : void{
        $gate->synchronized(static function() use ($gate, $owner, $spawned) : void{
            if($spawned && $gate['lost'] > 0){
                $slots = $gate['slots'];
                foreach(array_keys(iterator_to_array($slots)) as $key){
                    if($slots[$key] === self::LOST){
                        $slots[$key] = self::RELEASED;
                    }
                }
                $gate['calls'] = $gate['calls'] - $gate['lost'];
                $gate['lost'] = 0;
            }
            if($gate['pausedBy'] === $owner){
                $gate['pausedBy'] = 0;
            }
            $gate->notify();
        });
    }

    /**
     * Workers inside a call right now, across every pool.
     *
     * @param ThreadSafeArray<string, mixed> $gate
     */
    public static function calls(ThreadSafeArray $gate) : int{
        return $gate->synchronized(static fn() : int => $gate['calls'] - $gate['lost']);
    }

    /** @param ThreadSafeArray<string, mixed> $gate */
    private static function free(ThreadSafeArray $gate, int $key) : void{
        unset($gate['windows'][$key]);
        $slots = $gate['slots'];
        $state = $slots[$key] ?? 0;
        if($state === 0){
            return;
        }
        unset($slots[$key]);
        if($state !== self::RELEASED){
            $gate['calls'] = $gate['calls'] - 1;
            if($state === self::LOST){
                $gate['lost'] = $gate['lost'] - 1;
            }
            $gate->notify();
        }
    }
}
