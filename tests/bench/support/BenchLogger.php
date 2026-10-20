<?php

declare(strict_types=1);

namespace pRPCBench;

use pocketmine\thread\log\ThreadSafeLogger;

/** Prints warnings and worse, the AsyncPool needs a logger its workers can share. */
final class BenchLogger extends ThreadSafeLogger{
    public function emergency($message){ $this->log('emergency', $message); }
    public function alert($message){ $this->log('alert', $message); }
    public function critical($message){ $this->log('critical', $message); }
    public function error($message){ $this->log('error', $message); }
    public function warning($message){ $this->log('warning', $message); }
    public function notice($message){}
    public function info($message){}
    public function debug($message){}

    public function log($level, $message){
        fwrite(STDERR, "[$level] $message\n");
    }

    public function logException(\Throwable $e, $trace = null){
        $this->log('error', (string) $e);
    }
}
