<?php

declare(strict_types=1);

// Runs real worker threads against real ext-grpc and examples/backend (go run . in examples/backend).
// PRPC_PM=/path/to/PocketMine-MP php tests/grpc.php [filter]

putenv('PRPC_TRANSPORT=grpc');
require __DIR__ . '/bench/bootstrap.php';

use Google\Protobuf\StringValue;
use pmmp\thread\Thread as NativeThread;
use Trix\pRPC\opts\RpcClientConfig;
use Trix\pRPC\RpcClient;
use Trix\pRPC\RpcClientManager;
use Trix\pRPC\RpcMethod;
use Trix\pRPC\utils\exception\RpcException;

if(!extension_loaded('grpc')){
    fwrite(STDERR, "ext-grpc is not loaded.\n");
    exit(1);
}

$host = getenv('PRPC_HOST') ?: '127.0.0.1';
$echo = new RpcMethod('/test.Echo/Call', StringValue::class, StringValue::class);
$config = static fn(int $port, array $overrides = []) : RpcClientConfig => new RpcClientConfig(...array_merge([
    'endpoint' => "$host:$port",
    'methods' => [$echo],
    'workers' => 2,
    'defaultTimeoutMs' => 1_000,
], $overrides));

final class Log{
    public int $ok = 0;
    public int $wrong = 0;
    public int $sent = 0;
    /** @var array<string, int> */
    public array $err = [];

    public function settled() : bool{
        return $this->ok + array_sum($this->err) === $this->sent;
    }
}

final class Idle extends NativeThread{
    public function run() : void{}
}

$send = static function(RpcClient $client, string $value, Log $log) use ($echo) : void{
    ++$log->sent;
    $client->call($echo, new StringValue(['value' => $value]))->then(static function(StringValue $r) use ($value, $log) : void{
        ++$log->ok;
        if($r->getValue() !== $value){
            ++$log->wrong;
        }
    })->onError(static function(Throwable $e) use ($log) : void{
        $key = (new ReflectionClass($e))->getShortName() . ($e instanceof RpcException ? ':' . $e->getStatusCode() : '');
        $log->err[$key] = ($log->err[$key] ?? 0) + 1;
    });
};
$until = static function(Closure $done, float $maxSeconds = 10.0) : void{
    $end = hrtime(true) + (int) ($maxSeconds * 1e9);
    Bench::loop(static fn() : bool => !$done() && hrtime(true) < $end);
    if(!$done()){
        throw new RuntimeException('Timed out waiting for the condition.');
    }
};
$check = static function(bool $ok, string $what) : void{
    if(!$ok){
        throw new RuntimeException($what);
    }
};

$tests = [];

$tests['calls round trip and every response matches its request'] = static function() use ($config, $send, $until, $check) : void{
    $manager = new RpcClientManager(Bench::$plugin, workers: 4);
    $a = $manager->register('a', $config(50051));
    $b = $manager->register('b', $config(50052, ['batchSize' => 4]));
    $until(static fn() : bool => $a->isReady() && $b->isReady(), 5);
    $log = new Log();
    for($i = 0; $i < 2_000; ++$i){
        $send($i % 2 === 0 ? $a : $b, "v$i", $log);
    }
    $until($log->settled(...));
    $manager->close();
    $check($log->ok === 2_000 && $log->wrong === 0, json_encode($log));
};

$tests['threads started mid call do not hang single batch workers'] = static function() use ($config, $send, $until, $check) : void{
    // every new PHP thread replaces ext-grpc's completion queue, PocketMine restarts AsyncPool workers during play
    $manager = new RpcClientManager(Bench::$plugin, workers: 4);
    $slow = $manager->register('slow', $config(50100, ['workers' => 2, 'defaultTimeoutMs' => 2_000]));
    $fast = $manager->register('fast', $config(50051, ['workers' => 2]));
    $until(static fn() : bool => $slow->isReady() && $fast->isReady(), 5);
    $log = new Log();
    $spawned = 0;
    $tick = 0;
    $until(static function() use (&$tick, &$spawned, $slow, $fast, $send, $log) : bool{
        if(++$tick <= 60){
            // two workers on a 200ms backend take 10 calls a second
            if($tick % 4 === 0){
                $send($slow, "s$tick", $log);
            }
            for($i = 0; $i < 10; ++$i){
                $send($fast, "f$tick-$i", $log);
            }
            if($tick % 3 === 0){
                $t = new Idle();
                $t->start(NativeThread::INHERIT_NONE);
                $t->join();
                ++$spawned;
            }
        }
        return $tick > 60 && $log->settled();
    }, 20);
    $lost = $slow->stats()->lostWorkers;
    $manager->close();
    $check($spawned === 20, "spawned $spawned threads");
    $check($log->ok === $log->sent && $lost === 0, "$lost workers lost, " . json_encode($log));
};

$tests['more than 6 workers across clients never overrun ext-grpc'] = static function() use ($config, $send, $until, $check) : void{
    $clients = [];
    for($i = 0; $i < 4; ++$i){
        $clients[] = new RpcClient(Bench::$plugin, $config(50051 + $i, ['workers' => 3]));
    }
    $log = new Log();
    foreach($clients as $n => $client){
        for($i = 0; $i < 500; ++$i){
            $send($client, "c$n-$i", $log);
        }
    }
    $until($log->settled(...), 20);
    foreach($clients as $client){
        $client->close();
    }
    $check($log->ok === 2_000 && $log->wrong === 0, json_encode($log));
};

$tests['down, blackhole and flapping backends fail the way they should'] = static function() use ($config, $send, $until, $check) : void{
    $manager = new RpcClientManager(Bench::$plugin, workers: 4);
    $down = $manager->register('down', $config(50199, ['workers' => 1]));
    $dead = $manager->register('dead', $config(50101, ['workers' => 1, 'defaultTimeoutMs' => 300]));
    $flap = $manager->register('flap', $config(50102, ['workers' => 1, 'defaultTimeoutMs' => 500]));
    $downLog = new Log();
    $deadLog = new Log();
    $flapLog = new Log();
    $tick = 0;
    $until(static function() use (&$tick, $down, $dead, $flap, $send, $downLog, $deadLog, $flapLog) : bool{
        if(++$tick <= 60){
            $send($down, "d$tick", $downLog);
            $send($dead, "b$tick", $deadLog);
            $send($flap, "f$tick", $flapLog);
        }
        return $tick > 60 && $downLog->settled() && $deadLog->settled() && $flapLog->settled();
    }, 20);
    $manager->close();
    $check(($downLog->err['RpcException:14'] ?? 0) === 60, 'down: ' . json_encode($downLog->err));
    $check(($deadLog->err['RpcTimeoutException:4'] ?? 0) + ($deadLog->err['RpcException:4'] ?? 0) === 60, 'blackhole: ' . json_encode($deadLog->err));
    $check($flapLog->ok > 5 && $flapLog->ok < 60, 'flap: ' . json_encode($flapLog));
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
echo count($tests), " grpc tests, $failures failures\n";
exit($failures === 0 ? 0 : 1);
