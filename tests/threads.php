<?php

declare(strict_types=1);

// Runs real worker threads against the fake transport in tests/bench/fake.
// Needs a PHP build with pmmpthread and a PocketMine-MP checkout: PRPC_PM=/path/to/PocketMine-MP php tests/threads.php

require __DIR__ . '/bench/bootstrap.php';

use Google\Protobuf\StringValue;
use pmmp\thread\Thread as NativeThread;
use pocketmine\thread\ThreadManager;
use Trix\pRPC\opts\RpcClientConfig;
use Trix\pRPC\opts\RpcCredentials;
use Trix\pRPC\RpcClient;
use Trix\pRPC\RpcClientManager;
use Trix\pRPC\RpcMethod;
use Trix\pRPC\thread\GrpcGate;
use Trix\pRPC\utils\exception\RpcClosedException;
use Trix\pRPC\utils\exception\RpcException;
use Trix\pRPC\utils\exception\RpcOverloadedException;
use Trix\pRPC\utils\exception\RpcTimeoutException;
use Trix\pRPC\utils\exception\RpcWorkerException;

$echo = new RpcMethod('/test.Echo/Call', StringValue::class, StringValue::class);
$other = new RpcMethod('/test.Other/Call', StringValue::class, StringValue::class);

$config = static fn(string $target, array $overrides = []) : RpcClientConfig => new RpcClientConfig(...array_merge([
    'endpoint' => 'fake:' . $target,
    'methods' => [$echo],
    'workers' => 2,
    'defaultTimeoutMs' => 1_000,
], $overrides));

/** Runs the server loop with no tick work until $done returns true. */
$until = static function(Closure $done, float $maxSeconds = 10.0) : void{
    $end = hrtime(true) + (int) ($maxSeconds * 1e9);
    Bench::loop(static fn() : bool => !$done() && hrtime(true) < $end);
    if(!$done()){
        throw new RuntimeException('Timed out waiting for the condition.');
    }
};

// closes whatever a failed test left open so it can't leak threads into the next one
final class Open{
    /** @var list<RpcClient|RpcClientManager> */
    public static array $all = [];

    /** @template T of RpcClient|RpcClientManager @param T $x @return T */
    public static function add(RpcClient|RpcClientManager $x) : RpcClient|RpcClientManager{
        return self::$all[] = $x;
    }

    public static function closeAll() : void{
        foreach(self::$all as $x){
            $x->close(0);
        }
        self::$all = [];
    }
}

// an object so arrow functions see updates, they capture variables by value
final class CallLog{
    public int $ok = 0;
    /** @var array<string, int> */
    public array $err = [];
    /** @var list<float> */
    public array $lat = [];
    /** @var array<string, int> */
    public array $settled = [];
    public int $wrong = 0;
    public int $offThread = 0;
}

/** Sends one call and records how it settled, checking the echo belongs to this request. */
$send = static function(RpcClient $client, string $value, CallLog $log, ?RpcMethod $method = null) use ($echo) : void{
    $t = hrtime(true);
    $log->settled[$value] = 0;
    $client->call($method ?? $echo, new StringValue(['value' => $value]))->then(static function(StringValue $r) use ($value, $t, $log) : void{
        ++$log->settled[$value];
        ++$log->ok;
        $log->lat[] = (hrtime(true) - $t) / 1e6;
        if($r->getValue() !== $value){
            ++$log->wrong;
        }
        if(NativeThread::getCurrentThread() !== null){
            ++$log->offThread;
        }
    })->onError(static function(Throwable $e) use ($value, $log) : void{
        ++$log->settled[$value];
        $key = (new ReflectionClass($e))->getShortName() . ($e instanceof RpcException ? ':' . $e->getStatusCode() : '');
        $log->err[$key] = ($log->err[$key] ?? 0) + 1;
    });
};
$newLog = static fn() : CallLog => new CallLog();
$settled = static fn(CallLog $log) : bool => !in_array(0, $log->settled, true);
$check = static function(bool $ok, string $what) : void{
    if(!$ok){
        throw new RuntimeException($what);
    }
};
$exactlyOnce = static function(CallLog $log) use ($check) : void{
    $check($log->wrong === 0, "{$log->wrong} responses did not match their request");
    $check($log->offThread === 0, 'a callback ran off the main thread');
    foreach($log->settled as $value => $n){
        $check($n === 1, "$value settled $n times");
    }
};
$p99 = static fn(CallLog $log) : float => Bench::summary($log->lat)['p99'];
$pRPCThreads = static fn() : int => count(array_filter(ThreadManager::getInstance()->getAll(), static fn($t) => str_starts_with($t->getThreadName(), 'pRPC')));

$tests = [];

$tests['single endpoint, light and heavy traffic, every response matches its request'] = static function() use ($config, $send, $newLog, $settled, $until, $exactlyOnce, $check, $p99) : void{
    $client = Open::add(new RpcClient(Bench::$plugin, $config('lat=300,jit=300', ['maxOutstanding' => 10_000, 'batchSize' => 4])));
    $warm = $newLog();
    $send($client, 'warm-up', $warm);
    $until(static fn() : bool => $settled($warm));
    $log = $newLog();
    for($i = 0; $i < 20; ++$i){
        $send($client, "light-$i", $log);
        $until(static fn() : bool => $log->ok + array_sum($log->err) > $i);
    }
    $check($p99($log) < 20, 'light load p99 was ' . $p99($log) . 'ms');
    for($i = 0; $i < 5_000; ++$i){
        $send($client, "heavy-$i", $log);
    }
    $until(static fn() : bool => $settled($log));
    $exactlyOnce($log);
    $check($log->ok === 5_020 && $log->err === [], 'unexpected failures ' . json_encode($log->err));
    $check($client->outstandingCount() === 0 && $client->stats()->queuedBytes === 0, 'transport accounting leaked');
    $client->close();
};

$tests['2, 4, 8 and 16 endpoints on one manager stay correctly routed'] = static function() use ($config, $send, $newLog, $settled, $until, $exactlyOnce, $check, $pRPCThreads) : void{
    foreach([2, 4, 8, 16] as $n){
        $manager = Open::add(new RpcClientManager(Bench::$plugin, workers: 4));
        $log = $newLog();
        for($e = 0; $e < $n; ++$e){
            $manager->register("e$e", $config('lat=500,jit=2000', ['defaultTimeoutMs' => 4_000]));
        }
        $check($manager->threadCount() === 4 && $pRPCThreads() === 4, "$n endpoints should still use 4 threads");
        for($i = 0; $i < 200; ++$i){
            for($e = 0; $e < $n; ++$e){
                $send($manager->get("e$e"), "e$e-$i", $log);
            }
        }
        $until(static fn() : bool => $settled($log));
        $exactlyOnce($log);
        $check($log->ok === 200 * $n, "$n endpoints: " . json_encode($log->err));
        $manager->close();
        $check($pRPCThreads() === 0, 'manager left threads behind');
    }
};

$tests['two services on one endpoint keep their own method tables'] = static function() use ($config, $send, $newLog, $settled, $until, $exactlyOnce, $check, $echo, $other) : void{
    $manager = Open::add(new RpcClientManager(Bench::$plugin, workers: 2));
    $a = $manager->register('a', $config('lat=200'));
    $b = $manager->register('b', $config('lat=200', ['methods' => [$other]]));
    $log = $newLog();
    $send($a, 'a-1', $log);
    $send($b, 'b-1', $log, $other);
    $until(static fn() : bool => $settled($log));
    $exactlyOnce($log);
    $check($log->ok === 2, json_encode($log->err));
    try{
        $b->call($echo, new StringValue());
        $check(false, 'an unregistered method was accepted');
    }catch(InvalidArgumentException){
    }
    try{
        $manager->get('missing');
        $check(false, 'an unknown endpoint was returned');
    }catch(InvalidArgumentException){
    }
    $manager->close();
};

$tests['slow, dead and down endpoints do not slow a fast one'] = static function() use ($config, $send, $newLog, $settled, $until, $exactlyOnce, $check, $p99) : void{
    $manager = Open::add(new RpcClientManager(Bench::$plugin, workers: 4));
    $fast = $manager->register('fast', $config('lat=300'));
    $bad = [
        'slow' => $manager->register('slow', $config('lat=300000', ['defaultTimeoutMs' => 2_000])),
        'dead' => $manager->register('dead', $config('mode=blackhole', ['defaultTimeoutMs' => 1_500])),
        'down' => $manager->register('down', $config('mode=down')),
    ];
    $fastLog = $newLog();
    $badLog = $newLog();
    $tick = 0;
    $until(static function() use (&$tick, $fast, $bad, $send, &$fastLog, &$badLog) : bool{
        ++$tick;
        if($tick <= 60){
            $send($fast, "fast-$tick", $fastLog);
            foreach($bad as $name => $client){
                for($i = 0; $i < 5; ++$i){
                    $send($client, "$name-$tick-$i", $badLog);
                }
            }
        }
        return $tick > 60;
    });
    $until(static fn() : bool => $settled($fastLog) && $settled($badLog));
    $exactlyOnce($fastLog);
    $check($fastLog->ok === 60, 'fast endpoint failed: ' . json_encode($fastLog->err));
    $check($p99($fastLog) < 20, 'fast endpoint p99 was ' . $p99($fastLog) . 'ms next to bad endpoints');
    $check(($badLog->err['RpcException:14'] ?? 0) === 300, 'down endpoint should fail fast with UNAVAILABLE: ' . json_encode($badLog->err));
    $until(static fn() : bool => $bad['dead']->outstandingCount() === 0 && $bad['slow']->outstandingCount() === 0, 5);
    $manager->close();
};

$tests['a flood on one endpoint is shared fairly with a quiet one'] = static function() use ($config, $send, $newLog, $settled, $until, $check, $p99) : void{
    $manager = Open::add(new RpcClientManager(Bench::$plugin, workers: 2));
    $flood = $manager->register('flood', $config('lat=2000', ['workers' => 2, 'batchSize' => 1, 'maxOutstanding' => 10_000]));
    $quiet = $manager->register('quiet', $config('lat=300', ['workers' => 2]));
    $floodLog = $newLog();
    $quietLog = $newLog();
    for($i = 0; $i < 400; ++$i){
        $send($flood, "f-$i", $floodLog);
    }
    $tick = 0;
    $until(static function() use (&$tick, $quiet, $send, &$quietLog) : bool{
        if(++$tick <= 10){
            $send($quiet, "q-$tick", $quietLog);
        }
        return $tick > 10;
    });
    $until(static fn() : bool => $settled($quietLog) && $settled($floodLog), 20);
    // 400 calls of 2ms on two workers take ~400ms, the quiet lane gets the next free worker by round robin
    $check($quietLog->ok === 10 && $p99($quietLog) < 15, 'quiet endpoint p99 ' . $p99($quietLog) . 'ms behind a flood');
    $manager->close();
};

$tests['queue limits reject predictably'] = static function() use ($config, $send, $newLog, $settled, $until, $check) : void{
    $client = Open::add(new RpcClient(Bench::$plugin, $config('mode=blackhole', ['maxOutstanding' => 5, 'defaultTimeoutMs' => 200])));
    $log = $newLog();
    for($i = 0; $i < 8; ++$i){
        $send($client, "o-$i", $log);
    }
    $check(($log->err['RpcOverloadedException:8'] ?? 0) === 3, 'outstanding limit: ' . json_encode($log->err));
    $until(static fn() : bool => $settled($log));
    $client->close();

    $client = Open::add(new RpcClient(Bench::$plugin, $config('mode=blackhole', ['maxQueuedBytes' => 400, 'defaultTimeoutMs' => 200])));
    $log = $newLog();
    for($i = 0; $i < 8; ++$i){
        $send($client, str_pad("b-$i", 100, '.'), $log);
    }
    $check(($log->err['RpcOverloadedException:8'] ?? 0) > 0 && $client->stats()->queuedBytes <= 400, 'byte limit: ' . json_encode($log->err));
    $until(static fn() : bool => $settled($log));
    $client->close();

    // a single request over the byte budget still goes through when nothing else is queued
    $client = Open::add(new RpcClient(Bench::$plugin, $config('lat=100', ['maxQueuedBytes' => 64])));
    $log = $newLog();
    $send($client, str_repeat('x', 200), $log);
    $until(static fn() : bool => $settled($log));
    $check($log->ok === 1, 'oversized lone request: ' . json_encode($log->err));
    $client->close();
};

$tests['requests time out in the queue and in flight, and their slots come back'] = static function() use ($config, $send, $newLog, $settled, $until, $check, $exactlyOnce) : void{
    $client = Open::add(new RpcClient(Bench::$plugin, $config('mode=blackhole', ['workers' => 1, 'batchSize' => 1, 'defaultTimeoutMs' => 300])));
    $log = $newLog();
    for($i = 0; $i < 10; ++$i){
        $send($client, "t-$i", $log);
    }
    $until(static fn() : bool => $settled($log), 3);
    $exactlyOnce($log);
    // the worker reports DEADLINE_EXCEEDED the moment the local timer fires too, whichever lands first wins
    $check(($log->err['RpcTimeoutException:4'] ?? 0) + ($log->err['RpcException:4'] ?? 0) === 10, json_encode($log->err));
    $until(static fn() : bool => $client->outstandingCount() === 0, 3);
    $check($client->stats()->timeouts === 10, 'timeouts counter');
    $client->close();
};

$tests['an endpoint that flaps keeps recovering'] = static function() use ($config, $send, $newLog, $settled, $until, $check) : void{
    $client = Open::add(new RpcClient(Bench::$plugin, $config('lat=300,mode=flap,flap=200')));
    $log = $newLog();
    $tick = 0;
    $until(static function() use (&$tick, $client, $send, &$log) : bool{
        $send($client, 'f-' . ++$tick, $log);
        return $tick >= 40;
    });
    $until(static fn() : bool => $settled($log));
    $check($log->ok > 5 && ($log->err['RpcException:14'] ?? 0) > 5, 'expected both phases: ' . json_encode($log));
    $check($client->stats()->reconnects > 0, 'reconnects were not counted');
    $client->close();
};

$tests['isReady waits for the channel and stays false for a dead one'] = static function() use ($config, $until, $check) : void{
    $manager = Open::add(new RpcClientManager(Bench::$plugin, workers: 2));
    $ok = $manager->register('ok', $config('lat=100'));
    $down = $manager->register('down', $config('mode=down'));
    $until(static fn() : bool => $ok->isReady(), 2);
    $deadline = hrtime(true) + 300_000_000;
    while(hrtime(true) < $deadline){
        $check(!$down->isReady(), 'a dead endpoint reported ready');
        usleep(20_000);
    }
    $check($down->stats()->connectivity === 'transient_failure', 'connectivity was ' . $down->stats()->connectivity);
    $manager->close();
};

$tests['a worker stuck past its deadline is replaced and other endpoints keep working'] = static function() use ($config, $send, $newLog, $settled, $until, $check) : void{
    $manager = Open::add(new RpcClientManager(Bench::$plugin, workers: 2));
    $hung = $manager->register('hung', $config('mode=hang,hang=6000', ['workers' => 1, 'defaultTimeoutMs' => 300]));
    $ok = $manager->register('ok', $config('lat=200'));
    $hungLog = $newLog();
    $okLog = $newLog();
    $send($hung, 'h-1', $hungLog);
    $until(static fn() : bool => $settled($hungLog), 2);
    $check(isset($hungLog->err['RpcTimeoutException:4']), 'the stuck call did not time out locally');
    $until(static fn() : bool => $hung->stats()->lostWorkers === 1 && $hung->outstandingCount() === 0, 5);
    $check($manager->threadCount() === 2, 'the stuck worker was not replaced');
    for($i = 0; $i < 50; ++$i){
        $send($ok, "ok-$i", $okLog);
    }
    $until(static fn() : bool => $settled($okLog));
    $check($okLog->ok === 50, json_encode($okLog->err));
    $start = hrtime(true);
    $manager->close(100);
    $check((hrtime(true) - $start) / 1e6 < 1_000, 'close blocked on the stuck worker');
};

$tests['a crashed worker fails its calls and is replaced'] = static function() use ($config, $send, $newLog, $settled, $until, $check) : void{
    $manager = Open::add(new RpcClientManager(Bench::$plugin, workers: 2));
    $crash = $manager->register('crash', $config('mode=crash', ['workers' => 1, 'defaultTimeoutMs' => 3_000]));
    $ok = $manager->register('ok', $config('lat=200'));
    $log = $newLog();
    $send($crash, 'c-1', $log);
    $until(static fn() : bool => $settled($log), 5);
    $check(isset($log->err['RpcWorkerException:13']), 'expected a worker failure: ' . json_encode($log->err));
    $check($manager->threadCount() === 2, 'the crashed worker was not replaced');
    $okLog = $newLog();
    $send($ok, 'ok', $okLog);
    $until(static fn() : bool => $settled($okLog));
    $check($okLog->ok === 1, json_encode($okLog->err));
    $manager->close();
};

$tests['close with calls in flight settles each one once and stays within the grace period'] = static function() use ($config, $send, $newLog, $check, $exactlyOnce, $echo) : void{
    $manager = Open::add(new RpcClientManager(Bench::$plugin, workers: 2));
    $dead = $manager->register('dead', $config('mode=blackhole', ['defaultTimeoutMs' => 4_000, 'maxTimeoutMs' => 5_000]));
    $fast = $manager->register('fast', $config('lat=200'));
    $log = $newLog();
    for($i = 0; $i < 20; ++$i){
        $send($dead, "d-$i", $log);
        $send($fast, "f-$i", $log);
    }
    $start = hrtime(true);
    $manager->close(200);
    $closeMs = (hrtime(true) - $start) / 1e6;
    $exactlyOnce($log);
    $check(($log->err['RpcClosedException:1'] ?? 0) === 20 && $log->ok === 20, json_encode($log->err));
    $check($closeMs < 600, "close took {$closeMs}ms with a 200ms grace period");
    $late = 0;
    $dead->call($echo, new StringValue())->onError(static function(Throwable $e) use (&$late) : void{
        $late += $e instanceof RpcClosedException ? 1 : 100;
    });
    $check($late === 1, 'calls after close must reject with RpcClosedException');
    $before = clone $log;
    $end = hrtime(true) + 300_000_000;
    Bench::loop(static fn() : bool => hrtime(true) < $end);
    $check($log == $before, 'a callback fired after close');
};

$tests['closing one managed endpoint leaves the others running'] = static function() use ($config, $send, $newLog, $settled, $until, $check) : void{
    $manager = Open::add(new RpcClientManager(Bench::$plugin, workers: 2));
    $a = $manager->register('a', $config('lat=200'));
    $manager->register('b', $config('lat=200'));
    $manager->remove('a');
    $check($a->isClosed() && !$manager->has('a') && $manager->threadCount() === 2, 'remove broke the pool');
    $log = $newLog();
    $send($manager->get('b'), 'b', $log);
    $until(static fn() : bool => $settled($log));
    $check($log->ok === 1, json_encode($log->err));
    $manager->register('a', $config('lat=200'));
    $send($manager->get('a'), 'a-again', $log);
    $until(static fn() : bool => $settled($log));
    $check($log->ok === 2, 're-registering failed');
    $manager->close();
};

$tests['credentials never show up in stats or errors'] = static function() use ($config, $send, $newLog, $settled, $until, $check) : void{
    $secret = 'PRIVATE-KEY-MATERIAL-' . bin2hex(random_bytes(8));
    $client = Open::add(new RpcClient(Bench::$plugin, $config('mode=down', ['credentials' => RpcCredentials::tls('root', $secret, 'chain')])));
    $log = $newLog();
    $send($client, 'x', $log);
    $until(static fn() : bool => $settled($log));
    $check(!str_contains(print_r($client->stats(), true) . json_encode($log), $secret), 'secret leaked');
    $client->close();
};

$tests['20000 pending calls on a dead backend all time out and free their slots'] = static function() use ($config, $echo, $until, $check) : void{
    $after = [];
    // php's gc buffer and allocator settle unevenly, a leak would lift every round so the lowest one is compared
    foreach([20_000, 20_000, 20_000, 20_000] as $calls){
        $client = new RpcClient(Bench::$plugin, $config('mode=blackhole', ['maxOutstanding' => 30_000, 'defaultTimeoutMs' => 500]));
        $done = new CallLog();
        for($i = 0; $i < $calls; ++$i){
            $client->call($echo, new StringValue(['value' => 'x']))->onError(static function(Throwable $e) use ($done) : void{
                $e instanceof RpcTimeoutException || ($e instanceof RpcException && $e->getStatusCode() === 4) ? ++$done->ok : ++$done->wrong;
            });
        }
        $check($client->pendingCount() === $calls, 'pending ' . $client->pendingCount());
        $until(static fn() : bool => $done->ok + $done->wrong === $calls, 5);
        $until(static fn() : bool => $client->outstandingCount() === 0, 5);
        $check($done->wrong === 0, "{$done->wrong} failed with something other than a timeout");
        $client->close();
        unset($client);
        gc_collect_cycles();
        $after[] = memory_get_usage();
    }
    $grew = min(array_slice($after, 1)) - $after[0];
    $check($grew < 256 * 1024, "memory grew by $grew bytes over three more rounds");
};

$tests['every PocketMine async worker is started up front'] = static function() use ($check) : void{
    $async = Bench::$plugin->getServer()->getAsyncPool();
    $manager = Open::add(new RpcClientManager(Bench::$plugin, workers: 1));
    $check(count($async->getRunningWorkers()) === $async->getSize(), 'async workers left to start on demand');
    $manager->close();
};

$tests['two full managers never run more than 6 calls at once'] =static function() use ($config, $send, $newLog, $settled, $until, $exactlyOnce, $check) : void{
    $gate = GrpcGate::get();
    $peak = 0;
    $log = $newLog();
    $managers = [];
    foreach(['a', 'b'] as $name){
        $manager = $managers[] = Open::add(new RpcClientManager(Bench::$plugin, workers: 6));
        $client = $manager->register($name, $config('lat=2000', ['workers' => 6, 'maxOutstanding' => 10_000, 'defaultTimeoutMs' => 5_000]));
        for($i = 0; $i < 1_000; ++$i){
            $send($client, "$name-$i", $log);
        }
    }
    $until(static function() use (&$peak, $gate, $settled, $log) : bool{
        $peak = max($peak, GrpcGate::calls($gate));
        return $settled($log);
    }, 20);
    $exactlyOnce($log);
    $check($log->ok === 2_000, json_encode($log->err));
    $check($peak <= GrpcGate::MAX_CALLS && $peak >= 4, "peak of $peak calls in flight");
    foreach($managers as $manager){
        $manager->close();
    }
};

$tests['a lost worker is replaced at once while another pool has a long call in flight'] = static function() use ($config, $send, $newLog, $settled, $until, $check) : void{
    $busy = Open::add(new RpcClientManager(Bench::$plugin, workers: 1));
    $dead = $busy->register('dead', $config('mode=blackhole', ['workers' => 1, 'defaultTimeoutMs' => 4_000]));
    $deadLog = $newLog();
    $send($dead, 'dead', $deadLog);
    $until(static fn() : bool => GrpcGate::calls(GrpcGate::get()) === 1, 2);

    $manager = Open::add(new RpcClientManager(Bench::$plugin, workers: 2));
    $check($manager->threadCount() === 2, 'a new pool waited for a call in flight elsewhere');
    $crash = $manager->register('crash', $config('mode=crash', ['workers' => 1, 'defaultTimeoutMs' => 3_000]));
    $ok = $manager->register('ok', $config('lat=200'));
    $log = $newLog();
    $send($crash, 'c', $log);
    $until(static fn() : bool => $settled($log), 5);
    $start = hrtime(true);
    $until(static fn() : bool => $manager->threadCount() === 2, 2);
    $check((hrtime(true) - $start) / 1e6 < 500, 'replacement waited for the long call');
    $okLog = $newLog();
    $send($ok, 'ok', $okLog);
    $until(static fn() : bool => $settled($okLog));
    $check($okLog->ok === 1 && !$settled($deadLog), json_encode($okLog->err));
    $manager->close();
    $busy->close(0);
};

$tests['endpoints registered and removed under traffic leave no accounting behind'] = static function() use ($config, $send, $newLog, $settled, $until, $exactlyOnce, $check) : void{
    // a leaked slot in the shared limits would turn into overload errors on the endpoint that stays
    $manager = Open::add(new RpcClientManager(Bench::$plugin, workers: 3, maxOutstanding: 64));
    $keep = $manager->register('keep', $config('lat=300'));
    $keepLog = $newLog();
    $tmpLog = $newLog();
    for($round = 0; $round < 150; ++$round){
        $tmp = $manager->register("tmp$round", $config('lat=500,jit=500'));
        for($i = 0; $i < 5; ++$i){
            $send($tmp, "tmp$round-$i", $tmpLog);
        }
        $send($keep, "keep-$round", $keepLog);
        if($round % 3 === 0){
            $until(static fn() : bool => $settled($keepLog));
        }
        $manager->remove("tmp$round", $round % 2 === 0 ? 0 : 20);
    }
    $until(static fn() : bool => $settled($keepLog) && $settled($tmpLog));
    $exactlyOnce($keepLog);
    $exactlyOnce($tmpLog);
    $check($keepLog->ok === 150, 'kept endpoint failed: ' . json_encode($keepLog->err));
    $check(array_keys($tmpLog->err) === [] || array_keys($tmpLog->err) === ['RpcClosedException:1'], json_encode($tmpLog->err));
    $check($manager->names() === ['keep'] && $keep->outstandingCount() === 0, 'endpoints or calls left behind');
    $manager->close();
};

$tests['oversized and undecodable responses fail cleanly'] = static function() use ($config, $send, $newLog, $settled, $until, $check) : void{
    $big = Open::add(new RpcClient(Bench::$plugin, $config('lat=100', ['maxResponseBytes' => 100])));
    $bad = Open::add(new RpcClient(Bench::$plugin, $config('lat=100,mode=garbage')));
    $log = $newLog();
    $send($big, str_repeat('x', 200), $log);
    $send($big, 'small', $log);
    $send($bad, 'garbage', $log);
    $until(static fn() : bool => $settled($log));
    $check($log->ok === 1 && ($log->err['RpcException:13'] ?? 0) === 2, json_encode($log->err));
    $big->close();
    $bad->close();
};

$tests['a long run keeps memory flat'] = static function() use ($config, $echo, $until, $check) : void{
    $rounds = (int) (getenv('PRPC_LONG_ROUNDS') ?: 20);
    $manager = Open::add(new RpcClientManager(Bench::$plugin, workers: 4));
    $client = $manager->register('a', $config('lat=50', ['workers' => 4, 'maxOutstanding' => 10_000]));
    $done = new CallLog();
    $memory = 0;
    for($round = 0; $round < $rounds; ++$round){
        $target = ($round + 1) * 5_000;
        for($i = 0; $i < 5_000; ++$i){
            $client->call($echo, new StringValue(['value' => 'long']))
                ->then(static function() use ($done) : void{
                    ++$done->ok;
                })
                ->onError(static function() use ($done) : void{
                    ++$done->wrong;
                });
        }
        $until(static fn() : bool => $done->ok + $done->wrong >= $target, 30);
        if($round === 2){
            gc_collect_cycles();
            $memory = memory_get_usage();
        }
    }
    gc_collect_cycles();
    $grew = memory_get_usage() - $memory;
    $check($done->wrong === 0 && $done->ok === $rounds * 5_000, "{$done->wrong} calls failed");
    $check($client->outstandingCount() === 0 && $client->pendingCount() === 0, 'calls left behind');
    $check($grew < 256 * 1024, "memory grew by $grew bytes over " . ($rounds * 5_000) . ' calls');
    $manager->close();
};

$tests['repeated client creation and destruction leaves nothing behind'] =static function() use ($config, $send, $newLog, $settled, $until, $check, $pRPCThreads) : void{
    $memory = 0;
    // a stuck worker from an earlier test may still be finishing in the background
    $threadsBefore = $pRPCThreads();
    for($round = 0; $round < 30; ++$round){
        $client = Open::add(new RpcClient(Bench::$plugin, $config('lat=100')));
        $log = $newLog();
        $send($client, "r-$round", $log);
        $until(static fn() : bool => $settled($log));
        $client->close();
        if($round === 4){
            gc_collect_cycles();
            $memory = memory_get_usage();
        }
    }
    gc_collect_cycles();
    $check($pRPCThreads() <= $threadsBefore, 'threads leaked');
    $check(memory_get_usage() - $memory < 512 * 1024, 'memory grew by ' . (memory_get_usage() - $memory) . ' bytes');
};

$tests['every call slot comes back once background calls end'] = static function() use ($until) : void{
    $until(static fn() : bool => GrpcGate::calls(GrpcGate::get()) === 0, 10);
};

$failures = 0;
$only = $argv[1] ?? null;
foreach($tests as $name => $test){
    if($only !== null && array_filter(explode('|', $only), static fn(string $f) : bool => str_contains($name, $f)) === []){
        continue;
    }
    $start = hrtime(true);
    try{
        $test();
        printf("PASS %s (%.0f ms)\n", $name, (hrtime(true) - $start) / 1e6);
    }catch(Throwable $e){
        ++$failures;
        fwrite(STDERR, "FAIL $name: {$e->getMessage()}\n");
    }finally{
        Open::closeAll();
    }
}
echo count($tests), " thread tests, $failures failures\n";
exit($failures === 0 ? 0 : 1);
