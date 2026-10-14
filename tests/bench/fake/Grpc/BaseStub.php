<?php

declare(strict_types=1);

namespace Grpc;

require_once __DIR__ . '/consts.php';

/**
 * Stand-in for ext-grpc that keeps its blocking model: calls run concurrently once started, and wait() blocks
 * until that one call finishes. The target encodes the backend, e.g. "fake:lat=1000,jit=200,mode=ok".
 * Modes: ok, down (fast UNAVAILABLE), blackhole (hangs until the deadline), flap=<ms> (alternates ok and down),
 * hang with hang=<ms> (blocks past the deadline), crash (kills the worker thread), fail (channel setup throws).
 */
class BaseStub{
    /** @var array{lat: int, jit: int, mode: string, flap: int, cpu: int} */
    public array $model;
    private int $createdNs;

    /** @param array<string, mixed> $opts */
    public function __construct(string $hostname, array $opts){
        $model = ['lat' => 1000, 'jit' => 0, 'mode' => 'ok', 'flap' => 0, 'cpu' => 0];
        $spec = str_starts_with($hostname, 'fake:') ? substr($hostname, 5) : '';
        foreach(explode(',', $spec) as $pair){
            if($pair === ''){
                continue;
            }
            [$k, $v] = array_pad(explode('=', $pair, 2), 2, '');
            $model[$k] = is_numeric($v) ? (int) $v : $v;
        }
        if($model['mode'] === 'fail'){
            throw new \RuntimeException('fake channel creation failure');
        }
        $this->model = $model;
        $this->createdNs = hrtime(true);
    }

    public function down() : bool{
        return match($this->model['mode']){
            'down' => true,
            'flap' => intdiv(hrtime(true) - $this->createdNs, max(1, $this->model['flap']) * 1_000_000) % 2 === 1,
            default => false,
        };
    }

    public function getConnectivityState(bool $tryToConnect = false) : int{
        return $this->down() ? CHANNEL_TRANSIENT_FAILURE : CHANNEL_READY;
    }

    /** @param array<string, list<string>> $metadata @param array<string, mixed> $options */
    protected function _simpleRequest(string $method, object $argument, callable $deserialize, array $metadata = [], array $options = []) : UnaryCall{
        $call = new UnaryCall($this, $deserialize, (int) ($options['timeout'] ?? 1_000_000));
        $call->start($argument->serializeToString());
        return $call;
    }

    public function close() : void{}
}
