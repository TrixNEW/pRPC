<?php

declare(strict_types=1);

namespace Grpc;

require_once __DIR__ . '/consts.php';

/**
 * Keeps ext-grpc's blocking model: a call runs once its send ops are started, and a batch with
 * OP_RECV_STATUS_ON_CLIENT blocks until that call finishes.
 */
class Call{
    private int $doneNs = 0;
    private int $status = STATUS_OK;
    private string $bytes = '';

    public function __construct(private Channel $channel, string $method, private Timeval $deadline){}

    /** @param array<int, mixed> $ops */
    public function startBatch(array $ops) : \stdClass{
        $event = new \stdClass();
        if(isset($ops[OP_SEND_MESSAGE])){
            $this->start((string) $ops[OP_SEND_MESSAGE]['message']);
            $event->send_metadata = true;
            $event->send_message = true;
            $event->send_close = true;
        }
        if(isset($ops[OP_RECV_STATUS_ON_CLIENT])){
            $this->wait();
            $event->metadata = [];
            $event->message = $this->status !== STATUS_OK ? null : ($this->channel->model['mode'] === 'garbage' ? "\x0a\x64ab" : $this->bytes);
            $event->status = new \stdClass();
            $event->status->code = $this->status;
            $event->status->details = $this->status === STATUS_OK ? '' : 'fake status ' . $this->status;
            $event->status->metadata = [];
        }
        return $event;
    }

    private function start(string $bytes) : void{
        $m = $this->channel->model;
        self::spin($m['cpu']);
        $this->bytes = $bytes;
        $startNs = hrtime(true);
        $deadlineNs = $this->deadline->us * 1_000;
        $lat = $m['lat'] + ($m['jit'] > 0 ? random_int(0, $m['jit']) : 0);
        if($this->channel->down()){
            $this->status = STATUS_UNAVAILABLE;
            $lat = 200;
        }elseif($m['mode'] === 'blackhole'){
            $lat = PHP_INT_MAX >> 12;
        }
        $this->doneNs = $startNs + $lat * 1_000;
        if($this->doneNs > $deadlineNs){
            $this->doneNs = $deadlineNs;
            $this->status = STATUS_DEADLINE_EXCEEDED;
        }
    }

    private function wait() : void{
        $m = $this->channel->model;
        if($m['mode'] === 'crash'){
            // an uncatchable fatal, like a real crash
            ini_set('memory_limit', '16M');
            $grow = [];
            while(true){
                $grow[] = str_repeat('x', 1 << 20);
            }
        }
        if($m['mode'] === 'hang'){
            // what a call stuck on a replaced completion queue looks like, it ignores its deadline
            usleep(max(1, (int) ($m['hang'] ?? 5_000)) * 1_000);
        }
        // windows sleeps in ~15ms steps, so sleep coarse and spin the rest to keep latency accurate
        $left = $this->doneNs - hrtime(true);
        if($left > 20_000_000){
            usleep(intdiv($left - 20_000_000, 1_000));
        }
        while(hrtime(true) < $this->doneNs);
        self::spin($m['cpu']);
    }

    private static function spin(int $us) : void{
        if($us <= 0){
            return;
        }
        $end = hrtime(true) + $us * 1_000;
        while(hrtime(true) < $end);
    }
}
