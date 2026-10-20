<?php

declare(strict_types=1);

// Boots just enough of PocketMine to run pRPC outside a server.
// PRPC_PM points at a PocketMine-MP checkout with vendor/, PRPC_SRC at the pRPC src to load (defaults to this repo).
// PRPC_TRANSPORT=grpc uses the real ext-grpc against examples/backend, anything else uses tests/bench/fake.

use pocketmine\plugin\PluginBase;
use pocketmine\scheduler\AsyncPool;
use pocketmine\scheduler\TaskScheduler;
use pRPCBench\BenchLogger;
use pocketmine\Server;
use pocketmine\thread\ThreadSafeClassLoader;
use pocketmine\TimeTrackingSleeperHandler;
use pocketmine\timings\TimingsHandler;

$pm = getenv('PRPC_PM') ?: dirname(__DIR__, 3) . '/PocketMine';
$src = getenv('PRPC_SRC') ?: dirname(__DIR__, 2) . '/src/Trix/pRPC';
$fake = getenv('PRPC_TRANSPORT') !== 'grpc';

if(!is_file($pm . '/vendor/autoload.php')){
    fwrite(STDERR, "Set PRPC_PM to a PocketMine-MP checkout with composer dependencies installed.\n");
    exit(1);
}
require $pm . '/vendor/autoload.php';

$loader = new ThreadSafeClassLoader();
$loader->addPath('Trix\\pRPC', $src, true);
$loader->addPath('pRPCBench', __DIR__ . '/support', true);
if($fake){
    $loader->addPath('Grpc', __DIR__ . '/fake/Grpc', true);
}elseif(is_dir($lib = dirname(__DIR__, 2) . '/vendor/grpc/grpc/src/lib')){
    // pRPC up to 0.1.0-beta.1 used grpc/grpc's BaseStub on its workers
    $loader->addPath('Grpc', $lib, true);
}
require dirname(__DIR__, 2) . '/vendor/autoload.php';
// after composer, which prepends itself and would otherwise load this repo's src over PRPC_SRC
$loader->register(true);
require dirname(__DIR__) . '/grpc_constants.php';
if($fake){
    require __DIR__ . '/fake/Grpc/consts.php';
}

final class BenchPlugin extends PluginBase{}

final class Bench{
    public static TimeTrackingSleeperHandler $sleeper;
    public static TaskScheduler $scheduler;
    public static PluginBase $plugin;
    private static int $tick = 0;
    public static int $heartbeatNs = 0;

    public static function boot(ThreadSafeClassLoader $loader) : void{
        self::$sleeper = new TimeTrackingSleeperHandler(new TimingsHandler('pRPC bench'));
        self::$scheduler = new TaskScheduler();

        $serverRef = new ReflectionClass(Server::class);
        $server = $serverRef->newInstanceWithoutConstructor();
        $serverRef->getProperty('tickSleeper')->setValue($server, self::$sleeper);
        $serverRef->getProperty('autoloader')->setValue($server, $loader);
        $serverRef->getProperty('asyncPool')->setValue($server, new AsyncPool((int) (getenv('PRPC_ASYNC') ?: 2), 256, $loader, new BenchLogger(), self::$sleeper));
        $serverRef->getProperty('instance')->setValue(null, $server);

        $pluginRef = new ReflectionClass(PluginBase::class);
        $plugin = (new ReflectionClass(BenchPlugin::class))->newInstanceWithoutConstructor();
        $pluginRef->getProperty('server')->setValue($plugin, $server);
        $pluginRef->getProperty('scheduler')->setValue($plugin, self::$scheduler);
        self::$plugin = $plugin;
    }

    /**
     * Runs the server loop at 20 TPS: sleep until the next tick while handling notifications, then tick.
     * $tickWorkMs busy-waits inside each tick to stand in for game logic.
     *
     * @param Closure(int): bool $onTick return false to stop
     */
    public static function loop(Closure $onTick, float $tickWorkMs = 0.0, float $maxSeconds = 60.0) : void{
        $next = microtime(true);
        $end = $next + $maxSeconds;
        while($next < $end){
            self::$sleeper->sleepUntil($next);
            ++self::$tick;
            $t = hrtime(true);
            self::$scheduler->mainThreadHeartbeat(self::$tick);
            self::$heartbeatNs += hrtime(true) - $t;
            if(!$onTick(self::$tick)){
                return;
            }
            if($tickWorkMs > 0){
                $until = hrtime(true) + (int) ($tickWorkMs * 1_000_000);
                while(hrtime(true) < $until);
            }
            $next += 0.05;
            if($next < microtime(true) - 0.5){
                $next = microtime(true);
            }
        }
    }

    public static function threadCpuMs() : float{
        $r = getrusage(1);
        return ($r['ru_utime.tv_sec'] + $r['ru_stime.tv_sec']) * 1000 + ($r['ru_utime.tv_usec'] + $r['ru_stime.tv_usec']) / 1000;
    }

    public static function processCpuMs() : float{
        $r = getrusage();
        return ($r['ru_utime.tv_sec'] + $r['ru_stime.tv_sec']) * 1000 + ($r['ru_utime.tv_usec'] + $r['ru_stime.tv_usec']) / 1000;
    }

    public static function rssMb() : float{
        if(is_readable('/proc/self/status') && preg_match('/VmRSS:\s+(\d+)/', (string) file_get_contents('/proc/self/status'), $m) === 1){
            return (int) $m[1] / 1024;
        }
        $out = shell_exec('powershell -NoProfile -Command "(Get-Process -Id ' . getmypid() . ').WorkingSet64"');
        return is_string($out) && is_numeric(trim($out)) ? (int) trim($out) / 1048576 : 0.0;
    }

    /** @param list<float> $values @return array{n: int, p50: float, p95: float, p99: float, max: float} */
    public static function summary(array $values) : array{
        sort($values);
        $n = count($values);
        $at = static fn(float $q) : float => $n === 0 ? 0.0 : $values[min($n - 1, (int) floor($q * $n))];
        return ['n' => $n, 'p50' => $at(0.50), 'p95' => $at(0.95), 'p99' => $at(0.99), 'max' => $n === 0 ? 0.0 : $values[$n - 1]];
    }
}

Bench::boot($loader);
