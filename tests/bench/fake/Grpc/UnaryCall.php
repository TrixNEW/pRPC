<?php

declare(strict_types=1);

namespace Grpc;

require_once __DIR__ . '/consts.php';

class UnaryCall{
    private int $startNs = 0;
    private int $doneNs = 0;
    private int $deadlineNs = 0;
    private int $status = STATUS_OK;
    private string $bytes = '';

    /** @param callable(string): object $deserialize */
    public function __construct(private BaseStub $stub, private $deserialize, private int $timeoutUs){}

    public function start(string $bytes) : void{
        $m = $this->stub->model;
        self::spin($m['cpu']);
        $this->bytes = $bytes;
        $this->startNs = hrtime(true);
        $this->deadlineNs = $this->startNs + $this->timeoutUs * 1_000;
        $lat = $m['lat'] + ($m['jit'] > 0 ? random_int(0, $m['jit']) : 0);
        if($this->stub->down()){
            $this->status = STATUS_UNAVAILABLE;
            $lat = 200;
        }elseif($m['mode'] === 'blackhole'){
            $lat = PHP_INT_MAX >> 12;
        }
        $this->doneNs = $this->startNs + $lat * 1_000;
        if($this->doneNs > $this->deadlineNs){
            $this->doneNs = $this->deadlineNs;
            $this->status = STATUS_DEADLINE_EXCEEDED;
        }
    }

    /** @return array{0: ?object, 1: \stdClass} */
    public function wait() : array{
        $m = $this->stub->model;
        if($m['mode'] === 'crash'){
            // an uncatchable fatal, like a real crash
            ini_set('memory_limit', '16M');
            $grow = [];
            while(true){
                $grow[] = str_repeat('x', 1 << 20);
            }
        }
        if($m['mode'] === 'hang'){
            // what a call stuck on the wrong completion queue looks like, it ignores its deadline
            usleep(max(1, (int) ($m['hang'] ?? 5_000)) * 1_000);
        }
        // windows sleeps in ~15ms steps, so sleep coarse and spin the rest to keep latency accurate
        $left = $this->doneNs - hrtime(true);
        if($left > 20_000_000){
            usleep(intdiv($left - 20_000_000, 1_000));
        }
        while(hrtime(true) < $this->doneNs);
        self::spin($this->stub->model['cpu']);
        $status = new \stdClass();
        $status->code = $this->status;
        $status->details = $this->status === STATUS_OK ? '' : 'fake status ' . $this->status;
        $status->metadata = [];
        return [$this->status === STATUS_OK ? ($this->deserialize)($this->bytes) : null, $status];
    }

    private static function spin(int $us) : void{
        if($us <= 0){
            return;
        }
        $end = hrtime(true) + $us * 1_000;
        while(hrtime(true) < $end);
    }
}
