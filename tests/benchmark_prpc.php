<?php

declare(strict_types=1);

$autoloader = require 'phar://' . __DIR__ . '/PocketMine-MP.phar/vendor/autoload.php';

spl_autoload_register(function(string $class) : void{
    $prefixMap = [
        'Trix\\pRPC\\' => __DIR__ . '/plugins/pRPCTest/src/Trix/pRPC/',
        'Grpc\\' => __DIR__ . '/plugins/pRPCTest/src/Grpc/',
        'Data\\' => __DIR__ . '/plugins/pRPCTest/src/Data/',
        'GPBMetadata\\' => __DIR__ . '/plugins/pRPCTest/src/GPBMetadata/',
    ];

    foreach($prefixMap as $prefix => $baseDir){
        $len = strlen($prefix);
        if(strncmp($prefix, $class, $len) === 0){
            $relativeClass = substr($class, $len);
            $file = $baseDir . str_replace('\\', '/', $relativeClass) . '.php';
            if(file_exists($file)){
                require_once $file;
                return;
            }
        }
    }
});

use Data\GetPlayerRequest;
use Data\GetPlayerResponse;
use Data\SavePlayerRequest;
use Data\SavePlayerResponse;
use pocketmine\plugin\PluginBase;
use pocketmine\scheduler\TaskScheduler;
use pocketmine\snooze\SleeperHandler;
use Trix\pRPC\opts\RpcClientConfig;
use Trix\pRPC\opts\RpcCredentials;
use Trix\pRPC\RpcClient;
use Trix\pRPC\RpcMethod;

final class TestPlugin extends PluginBase{}

function createTestPlugin(SleeperHandler $sleeper, TaskScheduler $scheduler, object $autoloader) : PluginBase{
    $serverRef = new ReflectionClass(\pocketmine\Server::class);
    $server = $serverRef->newInstanceWithoutConstructor();

    if($serverRef->hasProperty('tickSleeper')){
        $propTick = $serverRef->getProperty('tickSleeper');
        $propTick->setAccessible(true);
        $propTick->setValue($server, $sleeper);
    }

    if($serverRef->hasProperty('autoloader')){
        $propAuto = $serverRef->getProperty('autoloader');
        $propAuto->setAccessible(true);
        $propAuto->setValue($server, new \pocketmine\thread\ThreadSafeClassLoader());
    }

    $propInstance = $serverRef->getProperty('instance');
    $propInstance->setAccessible(true);
    $propInstance->setValue(null, $server);

    $ref = new ReflectionClass(PluginBase::class);
    $plugin = (new ReflectionClass(TestPlugin::class))->newInstanceWithoutConstructor();

    if($ref->hasProperty('server')){
        $propServer = $ref->getProperty('server');
        $propServer->setAccessible(true);
        $propServer->setValue($plugin, $server);
    }

    if($ref->hasProperty('scheduler')){
        $propScheduler = $ref->getProperty('scheduler');
        $propScheduler->setAccessible(true);
        $propScheduler->setValue($plugin, $scheduler);
    }

    return $plugin;
}

$timings = new \pocketmine\timings\TimingsHandler("pRPC Benchmark");
$sleeper = new \pocketmine\TimeTrackingSleeperHandler($timings);
$scheduler = new TaskScheduler();
$plugin = createTestPlugin($sleeper, $scheduler, $autoloader);

$getPlayer = new RpcMethod("/data.DataService/GetPlayer", GetPlayerRequest::class, GetPlayerResponse::class);
$savePlayer = new RpcMethod("/data.DataService/SavePlayer", SavePlayerRequest::class, SavePlayerResponse::class);

function runTest(RpcClient $rpc, RpcMethod $method, SleeperHandler $sleeper, TaskScheduler $scheduler, int $totalCalls, string $testName) : void{
    echo ">>> Running {$testName} with {$totalCalls} calls...\n";

    $warmupDeadline = hrtime(true) + 5_000_000_000;
    while(hrtime(true) < $warmupDeadline){
        $sleeper->sleepUntil(microtime(true) + 0.005);
        $sleeper->processNotifications();
        $scheduler->mainThreadHeartbeat(1);
        $states = $rpc->stats()->workerStates;
        $readyCount = 0;
        foreach($states as $s){
            if($s === 'ready') ++$readyCount;
        }
        if($readyCount === count($states) && count($states) > 0){
            break;
        }
        usleep(5000);
    }

    $completed = 0;
    $success = 0;
    $failed = 0;
    $latenciesMs = [];

    $memStart = memory_get_usage(true);
    $startTime = hrtime(true);

    for($i = 0; $i < $totalCalls; ++$i){
        $req = new GetPlayerRequest();
        $req->setXuid((string) (1000000000 + $i));

        $callStart = hrtime(true);
        $rpc->call($method, $req)
            ->then(function(GetPlayerResponse $res) use (&$completed, &$success, &$latenciesMs, $callStart) : void{
                ++$completed;
                ++$success;
                $latenciesMs[] = (hrtime(true) - $callStart) / 1_000_000;
            })
            ->onError(function(Throwable $e) use (&$completed, &$failed, &$latenciesMs, $callStart) : void{
                ++$completed;
                ++$failed;
                $latenciesMs[] = (hrtime(true) - $callStart) / 1_000_000;
            });

        if(($i % 250) === 0){
            $sleeper->processNotifications();
        }
    }

    $deadlineWait = hrtime(true) + 30_000_000_000;
    $tickCount = 0;
    while($completed < $totalCalls && hrtime(true) < $deadlineWait){
        $sleeper->sleepUntil(microtime(true) + 0.001);
        $sleeper->processNotifications();
        $scheduler->mainThreadHeartbeat(++$tickCount);
    }

    $totalTimeMs = (hrtime(true) - $startTime) / 1_000_000;
    $totalTimeSec = max(0.0001, $totalTimeMs / 1000.0);
    $rps = $completed / $totalTimeSec;
    $memDeltaMb = (memory_get_usage(true) - $memStart) / (1024 * 1024);

    sort($latenciesMs);
    $count = count($latenciesMs);
    $min = $count > 0 ? $latenciesMs[0] : 0.0;
    $max = $count > 0 ? $latenciesMs[$count - 1] : 0.0;
    $avg = $count > 0 ? (array_sum($latenciesMs) / $count) : 0.0;
    $p50 = $count > 0 ? $latenciesMs[(int) ($count * 0.50)] : 0.0;
    $p95 = $count > 0 ? $latenciesMs[(int) ($count * 0.95)] : 0.0;
    $p99 = $count > 0 ? $latenciesMs[(int) ($count * 0.99)] : 0.0;

    echo "  [Results for {$testName}]\n";
    echo "  - Completed:   {$completed} / {$totalCalls} (Success: {$success}, Failed: {$failed})\n";
    echo "  - Total Time:  " . sprintf("%.2f ms (%.3f s)", $totalTimeMs, $totalTimeSec) . "\n";
    echo "  - Throughput:  " . sprintf("%.1f Calls / Second (RPS)", $rps) . "\n";
    echo "  - Latency Min: " . sprintf("%.2f ms", $min) . " | Avg: " . sprintf("%.2f ms", $avg) . " | Max: " . sprintf("%.2f ms", $max) . "\n";
    echo "  - Latency p50: " . sprintf("%.2f ms", $p50) . " | p95: " . sprintf("%.2f ms", $p95) . " | p99: " . sprintf("%.2f ms", $p99) . "\n";
    echo "  - Memory Delta: " . sprintf("%+.2f MB", $memDeltaMb) . "\n\n";
}

$configA = new RpcClientConfig(
    endpoint: "127.0.0.1:50051",
    methods: [$getPlayer, $savePlayer],
    credentials: RpcCredentials::insecure(),
    workers: 2,
    batchSize: 2,
    defaultTimeoutMs: 2000,
    maxTimeoutMs: 5000,
    maxOutstanding: 32768
);
$rpcA = new RpcClient($plugin, $configA);
runTest($rpcA, $getPlayer, $sleeper, $scheduler, 1000, "1,000 Burst Calls (2 Workers)");
$rpcA->close();

$configB = new RpcClientConfig(
    endpoint: "127.0.0.1:50051",
    methods: [$getPlayer, $savePlayer],
    credentials: RpcCredentials::insecure(),
    workers: 4,
    batchSize: 4,
    defaultTimeoutMs: 2000,
    maxTimeoutMs: 5000,
    maxOutstanding: 32768
);
$rpcB = new RpcClient($plugin, $configB);
runTest($rpcB, $getPlayer, $sleeper, $scheduler, 5000, "5,000 Burst Calls (4 Workers)");
$rpcB->close();

$configC = new RpcClientConfig(
    endpoint: "127.0.0.1:50051",
    methods: [$getPlayer, $savePlayer],
    credentials: RpcCredentials::insecure(),
    workers: 6,
    batchSize: 4,
    defaultTimeoutMs: 3000,
    maxTimeoutMs: 8000,
    maxOutstanding: 32768
);
$rpcC = new RpcClient($plugin, $configC);
runTest($rpcC, $getPlayer, $sleeper, $scheduler, 6000, "6,000 Burst Calls (6 Workers)");
$rpcC->close();
