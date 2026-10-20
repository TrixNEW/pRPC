<?php

declare(strict_types=1);

// php tests/bench/bench.php <scenario> [arch] [reps]
// arch: clients (one RpcClient per endpoint, works on any pRPC version) or manager (one shared RpcClientManager)

require __DIR__ . '/bootstrap.php';

use Google\Protobuf\StringValue;
use pocketmine\thread\ThreadManager;
use Trix\pRPC\opts\RpcClientConfig;
use Trix\pRPC\RpcClient;
use Trix\pRPC\RpcClientManager;
use Trix\pRPC\RpcMethod;

$fake = getenv('PRPC_TRANSPORT') !== 'grpc';
$host = getenv('PRPC_HOST') ?: '127.0.0.1';
$target = static function(int $port, string $fakeSpec) use ($fake, $host) : string{
    return $fake ? 'fake:' . $fakeSpec : "$host:$port";
};
// for the real backend: examples/backend serves ok on 50051+, slow on 50100, blackhole on 50101, flapping on 50102, down = nothing on 50199
$ep = static fn(string $name, string $target, float $perTick, int $timeoutMs = 1_500) : array => compact('name', 'target', 'perTick', 'timeoutMs');

$scenarios = [
    'light' => ['duration' => 5, 'tickWorkMs' => 5, 'endpoints' => [$ep('a', $target(50051, 'lat=500'), 10)]],
    'heavy' => ['duration' => 5, 'tickWorkMs' => 5, 'endpoints' => [$ep('a', $target(50051, 'lat=500'), 250)]],
    'burst' => ['burst' => 20_000, 'workers' => 6, 'batch' => 16, 'endpoints' => [$ep('a', $target(50051, 'lat=500'), 0, 5_000)]],
    'slow' => ['duration' => 5, 'tickWorkMs' => 5, 'endpoints' => [
        $ep('fast', $target(50051, 'lat=500'), 5),
        $ep('slow', $target(50100, 'lat=200000'), 10),
    ]],
    'blackhole' => ['duration' => 6, 'tickWorkMs' => 5, 'endpoints' => [
        $ep('fast', $target(50051, 'lat=500'), 5),
        $ep('dead', $target(50101, 'mode=blackhole'), 5),
    ]],
    'down' => ['duration' => 5, 'tickWorkMs' => 5, 'endpoints' => [
        $ep('fast', $target(50051, 'lat=500'), 5),
        $ep('down', $target(50199, 'mode=down'), 5),
    ]],
    'flap' => ['duration' => 6, 'tickWorkMs' => 5, 'endpoints' => [
        $ep('fast', $target(50051, 'lat=500'), 5),
        $ep('flap', $target(50102, 'lat=500,mode=flap,flap=500'), 5),
    ]],
];
foreach([2, 4, 8, 16] as $n){
    $scenarios["multi$n"] = ['duration' => 5, 'tickWorkMs' => 5, 'workers' => 1, 'endpoints' => array_map(
        static fn(int $i) => $ep("e$i", $target(50051 + $i, 'lat=1000,jit=500'), 20 / $n),
        range(0, $n - 1)
    )];
    $scenarios["idle$n"] = ['idle' => 10, 'workers' => 1, 'endpoints' => array_map(
        static fn(int $i) => $ep("e$i", $target(50051 + $i, 'lat=1000'), 0),
        range(0, $n - 1)
    )];
}

$name = $argv[1] ?? 'light';
$arch = $argv[2] ?? 'clients';
$reps = (int) ($argv[3] ?? 1);
$s = $scenarios[$name] ?? throw new InvalidArgumentException("unknown scenario $name, pick one of " . implode(', ', array_keys($scenarios)));
$method = new RpcMethod('/bench.Echo/Call', StringValue::class, StringValue::class);

for($rep = 1; $rep <= $reps; ++$rep){
    $rssBefore = Bench::rssMb();
    $startNs = hrtime(true);
    $clients = [];
    $manager = null;
    $config = static fn(array $e) : RpcClientConfig => new RpcClientConfig(
        endpoint: $e['target'],
        methods: [$method],
        workers: $s['workers'] ?? 2,
        batchSize: (int) (getenv('PRPC_BATCH') ?: ($s['batch'] ?? 4)),
        defaultTimeoutMs: $e['timeoutMs'],
        maxTimeoutMs: max(5_000, $e['timeoutMs']),
        maxOutstanding: 65_536
    );
    if($arch === 'manager'){
        $manager = new RpcClientManager(Bench::$plugin, workers: (int) (getenv('PRPC_POOL') ?: 4), maxOutstanding: 65_536);
        foreach($s['endpoints'] as $e){
            $clients[$e['name']] = $manager->register($e['name'], $config($e));
        }
    }else{
        foreach($s['endpoints'] as $e){
            $clients[$e['name']] = new RpcClient(Bench::$plugin, $config($e));
        }
    }
    $threads = count(ThreadManager::getInstance()->getAll());

    // the old isReady() flips once a worker made its stub, the new one once the channel reports READY
    $readyBy = hrtime(true) + 5_000_000_000;
    Bench::loop(static function() use ($clients, $readyBy) : bool{
        $ready = true;
        foreach($clients as $c){
            $ready = $c->isReady() && $ready;
        }
        return !$ready && hrtime(true) < $readyBy;
    });
    $startupMs = (hrtime(true) - $startNs) / 1e6;

    $lat = [];
    $errors = [];
    $sent = 0;
    $done = 0;
    $callNs = 0;
    $send = static function(string $name, RpcClient $c) use ($method, &$lat, &$errors, &$sent, &$done, &$callNs) : void{
        $req = new StringValue(['value' => str_repeat('x', 64)]);
        $t = hrtime(true);
        ++$sent;
        $promise = $c->call($method, $req);
        $callNs += hrtime(true) - $t;
        $promise->then(static function() use ($name, $t, &$lat, &$done) : void{
            $lat[$name][] = (hrtime(true) - $t) / 1e6;
            ++$done;
        })->onError(static function(Throwable $e) use ($name, &$errors, &$done) : void{
            $key = $name . ':' . (new ReflectionClass($e))->getShortName();
            $errors[$key] = ($errors[$key] ?? 0) + 1;
            ++$done;
        });
    };

    Bench::$sleeper->resetNotificationProcessingTime();
    Bench::$heartbeatNs = 0;
    $mainCpu = Bench::threadCpuMs();
    $procCpu = Bench::processCpuMs();
    $runNs = hrtime(true);
    if(isset($s['idle'])){
        $stopAt = hrtime(true) + $s['idle'] * 1_000_000_000;
        Bench::loop(static fn() : bool => hrtime(true) < $stopAt);
    }elseif(isset($s['burst'])){
        $fired = false;
        Bench::loop(static function() use (&$fired, $s, $clients, $send, &$done) : bool{
            if(!$fired){
                $fired = true;
                for($i = 0; $i < $s['burst']; ++$i){
                    $send('a', $clients['a']);
                }
            }
            return $done < $s['burst'];
        });
    }else{
        $acc = [];
        $stopAt = hrtime(true) + $s['duration'] * 1_000_000_000;
        Bench::loop(static function() use ($s, $clients, $send, &$acc, $stopAt, &$sent, &$done) : bool{
            if(hrtime(true) < $stopAt){
                foreach($s['endpoints'] as $e){
                    $acc[$e['name']] = ($acc[$e['name']] ?? 0.0) + $e['perTick'];
                    for(; $acc[$e['name']] >= 1.0; $acc[$e['name']] -= 1.0){
                        $send($e['name'], $clients[$e['name']]);
                    }
                }
                return true;
            }
            return $done < $sent && hrtime(true) < $stopAt + 6_000_000_000;
        }, $s['tickWorkMs']);
    }
    $runS = (hrtime(true) - $runNs) / 1e9;
    $mainCpu = Bench::threadCpuMs() - $mainCpu;
    $procCpu = Bench::processCpuMs() - $procCpu;
    $rssAfter = Bench::rssMb();

    $closeNs = hrtime(true);
    if($manager !== null){
        $manager->close();
    }else{
        foreach($clients as $c){
            $c->close();
        }
    }
    $closeMs = (hrtime(true) - $closeNs) / 1e6;

    $row = [
        'scenario' => $name, 'arch' => $arch, 'src' => getenv('PRPC_SRC') === false ? 'new' : basename(dirname(getenv('PRPC_SRC'), 3)), 'rep' => $rep,
        'threads' => $threads, 'startupMs' => round($startupMs, 1), 'closeMs' => round($closeMs, 1),
        'sent' => $sent, 'rps' => $runS > 0 ? round(($sent > 0 ? $sent : 0) / $runS) : 0,
        'mainCpuMs' => round($mainCpu),
        // wall time the main thread spent in call(), handling results and the tick task, callbacks included
        'mainPrpcMs' => round(($callNs + Bench::$sleeper->getNotificationProcessingTime() + Bench::$heartbeatNs) / 1e6, 1), 'procCpuMs' => round($procCpu), 'seconds' => round($runS, 2),
        'rssMb' => [round($rssBefore, 1), round($rssAfter, 1)], 'errors' => $errors,
    ];
    foreach($lat as $epName => $values){
        $row['lat'][$epName] = array_map(static fn(float $v) : float => round($v, 2), Bench::summary($values));
    }
    echo json_encode($row), "\n";
}
