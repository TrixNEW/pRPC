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

/**
 * Sends one call and records how it settled, checking the echo belongs to this request.
 *
 * @param array{ok: int, err: array<string, int>, lat: list<float>, settled: array<string, int>, wrong: int, offThread: int} $log
 */
$send = static function(RpcClient $client, string $value, array &$log, ?RpcMethod $method = null) use ($echo) : void{
    $t = hrtime(true);
    $log['settled'][$value] = 0;
    $client->call($method ?? $echo, new StringValue(['value' => $value]))->then(static function(StringValue $r) use ($value, $t, &$log) : void{
        ++$log['settled'][$value];
        ++$log['ok'];
        $log['lat'][] = (hrtime(true) - $t) / 1e6;
        if($r->getValue() !== $value){
            ++$log['wrong'];
        }
        if(NativeThread::getCurrentThread() !== null){
            ++$log['offThread'];
        }
    })->onError(static function(Throwable $e) use ($value, &$log) : void{
        ++$log['settled'][$value];
        $key = (new ReflectionClass($e))->getShortName() . ($e instanceof RpcException ? ':' . $e->getStatusCode() : '');
        $log['err'][$key] = ($log['err'][$key] ?? 0) + 1;
    });
};
$newLog = static fn() : array => ['ok' => 0, 'err' => [], 'lat' => [], 'settled' => [], 'wrong' => 0, 'offThread' => 0];
$settled = static fn(array $log) : bool => !in_array(0, $log['settled'], true);
$check = static function(bool $ok, string $what) : void{
    if(!$ok){
        throw new RuntimeException($what);
    }
};
$exactlyOnce = static function(array $log) use ($check) : void{
    $check($log['wrong'] === 0, "{$log['wrong']} responses did not match their request");
    $check($log['offThread'] === 0, 'a callback ran off the main thread');
    foreach($log['settled'] as $value => $n){
        $check($n === 1, "$value settled $n times");
    }
};
$p99 = static fn(array $log) : float => Bench::summary($log['lat'])['p99'];
$pRPCThreads = static fn() : int => count(array_filter(ThreadManager::getInstance()->getAll(), static fn($t) => str_starts_with($t->getThreadName(), 'pRPC')));

$tests = [];

$tests['single endpoint, light and heavy traffic, every response matches its request'] = static function() use ($config, $send, $newLog, $settled, $until, $exactlyOnce, $check, $p99) : void{
    $client = new RpcClient(Bench::$plugin, $config('lat=300,jit=300'));
    $log = $newLog();
    for($i = 0; $i < 20; ++$i){
        $send($client, "light-$i", $log);
        $until(static fn() : bool => $log['ok'] + array_sum($log['err']) > $i);
    }
    $check($p99($log) < 20, 'light load p99 was ' . $p99($log) . 'ms');
    for($i = 0; $i < 5_000; ++$i){
        $send($client, "heavy-$i", $log);
    }
    $until(static fn() : bool => $settled($log));
    $exactlyOnce($log);
    $check($log['ok'] === 5_020 && $log['err'] === [], 'unexpected failures ' . json_encode($log['err']));
    $check($client->outstandingCount() === 0 && $client->stats()->queuedBytes === 0, 'transport accounting leaked');
    $client->close();
};

$tests['2, 4, 8 and 16 endpoints on one manager stay correctly routed'] = static function() use ($config, $send, $newLog, $settled, $until, $exactlyOnce, $check, $pRPCThreads) : void{
    foreach([2, 4, 8, 16] as $n){
        $manager = new RpcClientManager(Bench::$plugin, workers: 4);
        $log = $newLog();
        for($e = 0; $e < $n; ++$e){
            $manager->register("e$e", $config('lat=500,jit=2000'));
        }
        $check($manager->threadCount() === 4 && $pRPCThreads() === 4, "$n endpoints should still use 4 threads");
        for($i = 0; $i < 200; ++$i){
            for($e = 0; $e < $n; ++$e){
                $send($manager->get("e$e"), "e$e-$i", $log);
            }
        }
        $until(static fn() : bool => $settled($log));
        $exactlyOnce($log);
        $check($log['ok'] === 200 * $n, "$n endpoints: " . json_encode($log['err']));
        $manager->close();
        $check($pRPCThreads() === 0, 'manager left threads behind');
    }
};

$tests['two services on one endpoint keep their own method tables'] = static function() use ($config, $send, $newLog, $settled, $until, $exactlyOnce, $check, $echo, $other) : void{
    $manager = new RpcClientManager(Bench::$plugin, workers: 2);
    $a = $manager->register('a', $config('lat=200'));
    $b = $manager->register('b', $config('lat=200', ['methods' => [$other]]));
    $log = $newLog();
    $send($a, 'a-1', $log);
    $send($b, 'b-1', $log, $other);
    $until(static fn() : bool => $settled($log));
    $exactlyOnce($log);
    $check($log['ok'] === 2, json_encode($log['err']));
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
    $manager = new RpcClientManager(Bench::$plugin, workers: 4);
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
    $check($fastLog['ok'] === 60, 'fast endpoint failed: ' . json_encode($fastLog['err']));
    $check($p99($fastLog) < 20, 'fast endpoint p99 was ' . $p99($fastLog) . 'ms next to bad endpoints');
    $check(($badLog['err']['RpcException:14'] ?? 0) === 300, 'down endpoint should fail fast with UNAVAILABLE: ' . json_encode($badLog['err']));
    $until(static fn() : bool => $bad['dead']->outstandingCount() === 0 && $bad['slow']->outstandingCount() === 0, 5);
    $manager->close();
};

$tests['a flood on one endpoint is shared fairly with a quiet one'] = static function() use ($config, $send, $newLog, $settled, $until, $check, $p99) : void{
    $manager = new RpcClientManager(Bench::$plugin, workers: 2);
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
    $check($quietLog['ok'] === 10 && $p99($quietLog) < 15, 'quiet endpoint p99 ' . $p99($quietLog) . 'ms behind a flood');
    $manager->close();
};

$tests['queue limits reject predictably'] = static function() use ($config, $send, $newLog, $settled, $until, $check) : void{
    $client = new RpcClient(Bench::$plugin, $config('mode=blackhole', ['maxOutstanding' => 5, 'defaultTimeoutMs' => 200]));
    $log = $newLog();
    for($i = 0; $i < 8; ++$i){
        $send($client, "o-$i", $log);
    }
    $check(($log['err']['RpcOverloadedException:8'] ?? 0) === 3, 'outstanding limit: ' . json_encode($log['err']));
    $until(static fn() : bool => $settled($log));
    $client->close();

    $client = new RpcClient(Bench::$plugin, $config('mode=blackhole', ['maxQueuedBytes' => 400, 'defaultTimeoutMs' => 200]));
    $log = $newLog();
    for($i = 0; $i < 8; ++$i){
        $send($client, str_pad("b-$i", 100, '.'), $log);
    }
    $check(($log['err']['RpcOverloadedException:8'] ?? 0) > 0 && $client->stats()->queuedBytes <= 400, 'byte limit: ' . json_encode($log['err']));
    $until(static fn() : bool => $settled($log));
    $client->close();
};

$tests['requests time out in the queue and in flight, and their slots come back'] = static function() use ($config, $send, $newLog, $settled, $until, $check, $exactlyOnce) : void{
    $client = new RpcClient(Bench::$plugin, $config('mode=blackhole', ['workers' => 1, 'batchSize' => 1, 'defaultTimeoutMs' => 300]));
    $log = $newLog();
    for($i = 0; $i < 10; ++$i){
        $send($client, "t-$i", $log);
    }
    $until(static fn() : bool => $settled($log), 3);
    $exactlyOnce($log);
    $check(($log['err']['RpcTimeoutException:4'] ?? 0) === 10, json_encode($log['err']));
    // nine of them expired before a worker reached them, the worker drops those without calling the server
    $until(static fn() : bool => $client->outstandingCount() === 0, 3);
    $check($client->stats()->timeouts === 10, 'timeouts counter');
    $client->close();
};

$tests['an endpoint that flaps keeps recovering'] = static function() use ($config, $send, $newLog, $settled, $until, $check) : void{
    $client = new RpcClient(Bench::$plugin, $config('lat=300,mode=flap,flap=200'));
    $log = $newLog();
    $tick = 0;
    $until(static function() use (&$tick, $client, $send, &$log) : bool{
        $send($client, 'f-' . ++$tick, $log);
        return $tick >= 40;
    });
    $until(static fn() : bool => $settled($log));
    $check($log['ok'] > 5 && ($log['err']['RpcException:14'] ?? 0) > 5, 'expected both phases: ' . json_encode($log));
    $check($client->stats()->reconnects > 0, 'reconnects were not counted');
    $client->close();
};

$tests['isReady waits for the channel and stays false for a dead one'] = static function() use ($config, $until, $check) : void{
    $manager = new RpcClientManager(Bench::$plugin, workers: 2);
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
    $manager = new RpcClientManager(Bench::$plugin, workers: 2);
    $hung = $manager->register('hung', $config('mode=hang,hang=6000', ['workers' => 1, 'defaultTimeoutMs' => 300]));
    $ok = $manager->register('ok', $config('lat=200'));
    $hungLog = $newLog();
    $okLog = $newLog();
    $send($hung, 'h-1', $hungLog);
    $until(static fn() : bool => $settled($hungLog), 2);
    $check(isset($hungLog['err']['RpcTimeoutException:4']), 'the stuck call did not time out locally');
    $until(static fn() : bool => $hung->stats()->lostWorkers === 1 && $hung->outstandingCount() === 0, 5);
    $check($manager->threadCount() === 2, 'the stuck worker was not replaced');
    for($i = 0; $i < 50; ++$i){
        $send($ok, "ok-$i", $okLog);
    }
    $until(static fn() : bool => $settled($okLog));
    $check($okLog['ok'] === 50, json_encode($okLog['err']));
    $start = hrtime(true);
    $manager->close(100);
    $check((hrtime(true) - $start) / 1e6 < 1_000, 'close blocked on the stuck worker');
};

$tests['a crashed worker fails its calls and is replaced'] = static function() use ($config, $send, $newLog, $settled, $until, $check) : void{
    $manager = new RpcClientManager(Bench::$plugin, workers: 2);
    $crash = $manager->register('crash', $config('mode=crash', ['workers' => 1, 'defaultTimeoutMs' => 3_000]));
    $ok = $manager->register('ok', $config('lat=200'));
    $log = $newLog();
    $send($crash, 'c-1', $log);
    $until(static fn() : bool => $settled($log), 5);
    $check(isset($log['err']['RpcWorkerException:2']), 'expected a worker failure: ' . json_encode($log['err']));
    $check($manager->threadCount() === 2, 'the crashed worker was not replaced');
    $okLog = $newLog();
    $send($ok, 'ok', $okLog);
    $until(static fn() : bool => $settled($okLog));
    $check($okLog['ok'] === 1, json_encode($okLog['err']));
    $manager->close();
};

$tests['close with calls in flight settles each one once and stays within the grace period'] = static function() use ($config, $send, $newLog, $check, $exactlyOnce, $echo) : void{
    $manager = new RpcClientManager(Bench::$plugin, workers: 2);
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
    $check(($log['err']['RpcClosedException:2'] ?? 0) === 20 && $log['ok'] === 20, json_encode($log['err']));
    $check($closeMs < 600, "close took {$closeMs}ms with a 200ms grace period");
    $late = 0;
    $dead->call($echo, new StringValue())->onError(static function(Throwable $e) use (&$late) : void{
        $late += $e instanceof RpcClosedException ? 1 : 100;
    });
    $check($late === 1, 'calls after close must reject with RpcClosedException');
    $before = $log;
    $end = hrtime(true) + 300_000_000;
    Bench::loop(static fn() : bool => hrtime(true) < $end);
    $check($log === $before, 'a callback fired after close');
};

$tests['closing one managed endpoint leaves the others running'] = static function() use ($config, $send, $newLog, $settled, $until, $check) : void{
    $manager = new RpcClientManager(Bench::$plugin, workers: 2);
    $a = $manager->register('a', $config('lat=200'));
    $manager->register('b', $config('lat=200'));
    $manager->remove('a');
    $check($a->isClosed() && !$manager->has('a') && $manager->threadCount() === 2, 'remove broke the pool');
    $log = $newLog();
    $send($manager->get('b'), 'b', $log);
    $until(static fn() : bool => $settled($log));
    $check($log['ok'] === 1, json_encode($log['err']));
    $manager->register('a', $config('lat=200'));
    $send($manager->get('a'), 'a-again', $log);
    $until(static fn() : bool => $settled($log));
    $check($log['ok'] === 2, 're-registering failed');
    $manager->close();
};

$tests['credentials never show up in stats or errors'] = static function() use ($config, $send, $newLog, $settled, $until, $check) : void{
    $secret = 'PRIVATE-KEY-MATERIAL-' . bin2hex(random_bytes(8));
    $client = new RpcClient(Bench::$plugin, $config('mode=down', ['credentials' => RpcCredentials::tls('root', $secret, 'chain')]));
    $log = $newLog();
    $send($client, 'x', $log);
    $until(static fn() : bool => $settled($log));
    $check(!str_contains(print_r($client->stats(), true) . json_encode($log), $secret), 'secret leaked');
    $client->close();
};

$tests['repeated client creation and destruction leaves nothing behind'] = static function() use ($config, $send, $newLog, $settled, $until, $check, $pRPCThreads) : void{
    $memory = 0;
    for($round = 0; $round < 30; ++$round){
        $client = new RpcClient(Bench::$plugin, $config('lat=100'));
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
    $check($pRPCThreads() === 0, 'threads leaked');
    $check(memory_get_usage() - $memory < 512 * 1024, 'memory grew by ' . (memory_get_usage() - $memory) . ' bytes');
};

$failures = 0;
$only = $argv[1] ?? null;
foreach($tests as $name => $test){
    if($only !== null && !str_contains($name, $only)){
        continue;
    }
    $start = hrtime(true);
    try{
        $test();
        printf("PASS %s (%.0f ms)\n", $name, (hrtime(true) - $start) / 1e6);
    }catch(Throwable $e){
        ++$failures;
        fwrite(STDERR, "FAIL $name: {$e->getMessage()}\n");
    }
}
echo count($tests), " thread tests, $failures failures\n";
exit($failures === 0 ? 0 : 1);
