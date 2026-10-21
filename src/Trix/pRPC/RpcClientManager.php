<?php

declare(strict_types=1);

namespace Trix\pRPC;

use InvalidArgumentException;
use pocketmine\plugin\PluginBase;
use Trix\pRPC\internal\RpcWorkerPool;
use Trix\pRPC\opts\RpcClientConfig;
use Trix\pRPC\stats\RpcClientStats;

/**
 * Named endpoints on one shared, fixed set of worker threads.
 *
 * Each endpoint keeps its own methods, credentials, deadlines and limits, and RpcClientConfig::$workers caps how many
 * of the shared workers it may hold, so a slow or dead endpoint can't starve the others. Endpoints with the same
 * address, credentials and channel options share one gRPC channel. The manager owns the workers and the clients it
 * registers, closing it closes all of them.
 */
final class RpcClientManager{
    private readonly RpcWorkerPool $pool;

    /** @var array<string, RpcClient> */
    private array $clients = [];

    /**
     * @param int $workers threads shared by every endpoint, at most 6, or 16 on an ext-grpc with a queue per thread
     * @param int $maxOutstanding requests in flight across every endpoint
     * @param int $maxQueuedBytes request bytes queued or in flight across every endpoint
     */
    public function __construct(PluginBase $plugin, int $workers = 4, int $maxOutstanding = 8_192, int $maxQueuedBytes = 67_108_864){
        $this->pool = new RpcWorkerPool($plugin, $workers, $maxOutstanding, $maxQueuedBytes);
    }

    public function register(string $name, RpcClientConfig $config) : RpcClient{
        if($name === ''){
            throw new InvalidArgumentException('Endpoint name cannot be empty.');
        }
        if($this->has($name)){
            throw new InvalidArgumentException("Endpoint '$name' is already registered.");
        }
        return $this->clients[$name] = new RpcClient($this->pool, $config, $name);
    }

    public function has(string $name) : bool{
        $client = $this->clients[$name] ?? null;
        if($client !== null && $client->isClosed()){
            unset($this->clients[$name]);
            return false;
        }
        return $client !== null;
    }

    public function get(string $name) : RpcClient{
        if(!$this->has($name)){
            throw new InvalidArgumentException("Endpoint '$name' is not registered.");
        }
        return $this->clients[$name];
    }

    /** @return list<string> */
    public function names() : array{
        return array_values(array_filter(array_keys($this->clients), $this->has(...)));
    }

    /** Closes one endpoint, the workers and every other endpoint keep running. */
    public function remove(string $name, int $gracePeriodMs = 500) : void{
        if($this->has($name)){
            $this->clients[$name]->close($gracePeriodMs);
            unset($this->clients[$name]);
        }
    }

    /** @return array<string, RpcClientStats> */
    public function stats() : array{
        $stats = [];
        foreach($this->names() as $name){
            $stats[$name] = $this->clients[$name]->stats();
        }
        return $stats;
    }

    public function threadCount() : int{
        return $this->pool->liveWorkers();
    }

    /** Blocks the thread, so only use this at startup or shutdown. */
    public function drain(int $timeoutMs) : bool{
        $deadline = hrtime(true) + max(0, $timeoutMs) * 1_000_000;
        foreach($this->names() as $name){
            if(!$this->clients[$name]->drain(max(0, intdiv($deadline - hrtime(true), 1_000_000)))){
                return false;
            }
        }
        return true;
    }

    public function isClosed() : bool{
        return $this->pool->isClosed();
    }

    /** Closes every endpoint and stops the workers, see RpcClient::close() for what the grace period covers. */
    public function close(int $gracePeriodMs = 500) : void{
        if($gracePeriodMs < 0){
            throw new InvalidArgumentException('gracePeriodMs cannot be negative.');
        }
        $this->pool->close($gracePeriodMs);
        $this->clients = [];
    }
}
