<?php

declare(strict_types=1);

require dirname(__DIR__) . '/vendor/autoload.php';
require __DIR__ . '/grpc_constants.php';

use Google\Protobuf\Internal\Message;
use Trix\pRPC\opts\RpcCallOptions;
use Trix\pRPC\opts\RpcClientConfig;
use Trix\pRPC\RpcMethod;
use Trix\pRPC\retry\RpcRetrier;
use Trix\pRPC\retry\RpcRetryPolicy;
use Trix\pRPC\retry\RpcStatus;
use Trix\pRPC\utils\exception\RpcClosedException;
use Trix\pRPC\utils\exception\RpcException;
use Trix\pRPC\utils\exception\RpcMessageTooLargeException;
use Trix\pRPC\utils\exception\RpcOverloadedException;
use Trix\pRPC\utils\exception\RpcTimeoutException;
use Trix\pRPC\utils\exception\RpcWorkerException;
use Vow\CancelledException;
use Vow\Deferred;
use Vow\Promise;

final class TestMessage extends Message{}

$tests = [];
$test = static function(string $name, Closure $callback) use (&$tests) : void{
    $tests[$name] = $callback;
};
$throws = static function(string $class, Closure $callback) : void{
    try{
        $callback();
    }catch(Throwable $e){
        if($e instanceof $class){
            return;
        }
        throw new RuntimeException("Expected $class, got " . $e::class, 0, $e);
    }
    throw new RuntimeException("Expected $class to be thrown.");
};

$test('Composer autoload resolves production classes', static function() : void{
    if(!class_exists(RpcCallOptions::class)){
        throw new RuntimeException('RpcCallOptions was not autoloaded.');
    }
});

$test('metadata is normalized and measured', static function() : void{
    $options = new RpcCallOptions(metadata: ['Authorization' => 'abc', 'x-bin' => ["\0\1"]]);
    if(array_keys($options->metadata) !== ['authorization', 'x-bin'] || $options->metadataBytes() !== 23){
        throw new RuntimeException('Unexpected normalized metadata or byte count.');
    }
});

$test('case-colliding metadata is rejected', static function() use ($throws) : void{
    $throws(InvalidArgumentException::class, static fn() => new RpcCallOptions(
        metadata: ['X-Key' => 'one', 'x-key' => 'two']
    ));
});

$test('channel message limits cannot bypass allocation bounds', static function() : void{
    $method = new RpcMethod('/test.Service/Call', TestMessage::class, TestMessage::class);
    $config = new RpcClientConfig(
        endpoint: 'localhost:50051',
        methods: [$method],
        channelOptions: [
            'grpc.max_send_message_length' => 10_000,
            'grpc.max_receive_message_length' => 20_000,
        ],
        maxRequestBytes: 100,
        maxResponseBytes: 200
    );
    if($config->channelOptions['grpc.max_send_message_length'] !== 100
        || $config->channelOptions['grpc.max_receive_message_length'] !== 200){
        throw new RuntimeException('Channel limits exceeded configured allocation bounds.');
    }
});

$test('failures are classified by whether a retry is safe', static function() : void{
    $cases = [
        [new RpcOverloadedException(10), true, false],
        [new RpcMessageTooLargeException('request', 10, 5), false, false],
        [new RpcTimeoutException(100), true, true],
        [new RpcClosedException(), false, true],
        [new RpcWorkerException('lost mid call'), true, true],
        [new RpcWorkerException('never queued', sent: false), true, false],
        [new RpcException('down', RpcStatus::UNAVAILABLE), true, true],
        [new RpcException('busy', RpcStatus::RESOURCE_EXHAUSTED), true, false],
        [new RpcException('rule', RpcStatus::FAILED_PRECONDITION), false, false],
        [new RpcException('bad', RpcStatus::INVALID_ARGUMENT), false, false],
        [new RuntimeException('other'), false, true],
    ];
    foreach($cases as [$e, $retryable, $ambiguous]){
        if(RpcStatus::isRetryable($e) !== $retryable || RpcStatus::isAmbiguous($e) !== $ambiguous){
            throw new RuntimeException('Wrong classification for ' . $e::class . ': ' . $e->getMessage());
        }
    }
});

$test('retry delays grow, cap and stay inside the jitter band', static function() : void{
    $p = new RpcRetryPolicy(maxAttempts: 10, baseDelayMs: 100, maxDelayMs: 1_000, jitter: 0.2);
    if($p->delayMs(1, 0.5) !== 100 || $p->delayMs(2, 0.5) !== 200 || $p->delayMs(5, 0.5) !== 1_000 || $p->delayMs(40, 0.5) !== 1_000){
        throw new RuntimeException('Unexpected backoff.');
    }
    if($p->delayMs(1, 0.0) !== 80 || $p->delayMs(1, 1.0) !== 120 || $p->delayMs(5, 1.0) !== 1_000){
        throw new RuntimeException('Unexpected jitter.');
    }
});

$retrier = static function() : array{
    $now = 0;
    $cancelled = new ArrayObject();
    $errors = new ArrayObject();
    $r = new RpcRetrier(
        static function(Throwable $e) use ($errors) : void{ $errors[] = $e; },
        static function() use (&$now) : int{ return $now; }
    );
    $cancelAttempt = static function(Promise $p) use ($cancelled) : bool{ $cancelled[] = $p; return true; };
    return [$r, static function(int $ms) use (&$now, $r) : void{ $now += $ms * 1_000_000; $r->tick(); }, $cancelled, $errors, $cancelAttempt];
};

$attempts = static function(array $outcomes, int &$calls, ?ArrayObject $deferreds = null) : Closure{
    return static function() use (&$outcomes, &$calls, $deferreds) : Promise{
        ++$calls;
        $next = array_shift($outcomes);
        if($next === null){
            $d = new Deferred();
            $deferreds[] = $d;
            return $d->promise;
        }
        return $next instanceof Throwable ? Promise::rejected($next) : Promise::resolved($next);
    };
};

$test('retrier resends transient failures after the delay and resolves once', static function() use ($retrier, $attempts) : void{
    [$r, $advance] = $retrier();
    $calls = 0;
    $result = null;
    $retries = [];
    $r->run(
        $attempts([new RpcTimeoutException(5), new RpcException('down', RpcStatus::UNAVAILABLE), 'ok'], $calls),
        new RpcRetryPolicy(maxAttempts: 5, baseDelayMs: 10, maxDelayMs: 100, retryAmbiguous: true, jitter: 0.0),
        static function(Throwable $e, int $n) use (&$retries) : void{ $retries[] = $n; }
    )->then(static function(string $v) use (&$result) : void{ $result = $v; });
    if($calls !== 1 || $r->waiting() !== 1){
        throw new RuntimeException('First retry was not scheduled.');
    }
    $advance(9);
    if($calls !== 1){
        throw new RuntimeException('Retry ran before its delay.');
    }
    $advance(1);
    $advance(20);
    if($calls !== 3 || $result !== 'ok' || $retries !== [1, 2] || $r->waiting() !== 0){
        throw new RuntimeException("Unexpected retry run: calls=$calls");
    }
});

$test('retrier stops on final errors, attempt limit and ambiguous failures when not allowed', static function() use ($retrier, $attempts) : void{
    [$r, $advance] = $retrier();
    $calls = 0;
    $error = null;
    $r->run($attempts([new RpcException('rule', RpcStatus::FAILED_PRECONDITION)], $calls), new RpcRetryPolicy())
        ->catch(static function(Throwable $e) use (&$error) : void{ $error = $e; });
    if($calls !== 1 || $error === null || $r->waiting() !== 0){
        throw new RuntimeException('Final error was retried.');
    }

    $calls = 0;
    $error = null;
    $r->run($attempts(array_fill(0, 5, new RpcOverloadedException(1)), $calls), new RpcRetryPolicy(maxAttempts: 3, baseDelayMs: 1, maxDelayMs: 1, jitter: 0.0))
        ->catch(static function(Throwable $e) use (&$error) : void{ $error = $e; });
    $advance(1);
    $advance(1);
    $advance(1);
    if($calls !== 3 || !$error instanceof RpcOverloadedException){
        throw new RuntimeException("Attempt limit ignored: calls=$calls");
    }

    $calls = 0;
    $error = null;
    $r->run($attempts([new RpcTimeoutException(1), 'ok'], $calls), new RpcRetryPolicy(retryAmbiguous: false))
        ->catch(static function(Throwable $e) use (&$error) : void{ $error = $e; });
    if($calls !== 1 || !$error instanceof RpcTimeoutException){
        throw new RuntimeException('Ambiguous failure was retried.');
    }
});

$test('retrier gives ambiguous final codes a few tries and never retries a closed client', static function() use ($retrier, $attempts) : void{
    [$r, $advance] = $retrier();
    $calls = 0;
    $result = null;
    $policy = new RpcRetryPolicy(maxAttempts: 10, baseDelayMs: 1, maxDelayMs: 1, retryAmbiguous: true, jitter: 0.0);
    $r->run($attempts([new RpcException('reset', RpcStatus::INTERNAL), new RpcTimeoutException(1), 'ok'], $calls), $policy)
        ->then(static function(string $v) use (&$result) : void{ $result = $v; });
    $advance(1);
    $advance(1);
    if($calls !== 3 || $result !== 'ok'){
        throw new RuntimeException("Ambiguous INTERNAL was not retried: calls=$calls");
    }

    $calls = 0;
    $error = null;
    $r->run($attempts(array_fill(0, 10, new RpcException('bad', RpcStatus::INTERNAL)), $calls), $policy)
        ->catch(static function(Throwable $e) use (&$error) : void{ $error = $e; });
    for($i = 0; $i < 10; ++$i){
        $advance(1);
    }
    if($calls !== 3 || $error === null){
        throw new RuntimeException("Uncertain attempt limit ignored: calls=$calls");
    }

    $calls = 0;
    $error = null;
    $r->run($attempts([new RpcException('reset', RpcStatus::INTERNAL), 'ok'], $calls), new RpcRetryPolicy())
        ->catch(static function(Throwable $e) use (&$error) : void{ $error = $e; });
    if($calls !== 1 || $error === null){
        throw new RuntimeException('Default policy retried an ambiguous failure.');
    }

    $calls = 0;
    $error = null;
    $r->run($attempts([new RpcClosedException(), 'ok'], $calls), $policy)
        ->catch(static function(Throwable $e) use (&$error) : void{ $error = $e; });
    if($calls !== 1 || !$error instanceof RpcClosedException){
        throw new RuntimeException('Closed client was retried.');
    }
});

$test('stopping the retrier runs waiting retries immediately', static function() use ($retrier, $attempts) : void{
    [$r] = $retrier();
    $calls = 0;
    $error = null;
    $r->run($attempts([new RpcOverloadedException(1), new RpcClosedException()], $calls), new RpcRetryPolicy(baseDelayMs: 1_000, maxDelayMs: 1_000))
        ->catch(static function(Throwable $e) use (&$error) : void{ $error = $e; });
    $r->stop();
    if($calls !== 2 || !$error instanceof RpcClosedException || $r->waiting() !== 0){
        throw new RuntimeException('Stop did not settle waiting retries.');
    }
});

$test('cancelling a retry run cancels its attempt in flight and ignores the late result', static function() use ($retrier, $attempts) : void{
    [$r, , $cancelled, , $cancelAttempt] = $retrier();
    $calls = 0;
    $deferreds = new ArrayObject();
    $error = null;
    $promise = $r->run($attempts([null], $calls, $deferreds), new RpcRetryPolicy());
    $promise->catch(static function(Throwable $e) use (&$error) : void{ $error = $e; });
    if(!$r->cancel($promise, $cancelAttempt) || $r->cancel($promise, $cancelAttempt)){
        throw new RuntimeException('Cancel was not exactly once.');
    }
    if(!$error instanceof CancelledException || count($cancelled) !== 1 || $cancelled[0] !== $deferreds[0]->promise){
        throw new RuntimeException('Cancel did not reject the run or reach the attempt in flight.');
    }
    $deferreds[0]->resolve('late');
    if(!$promise->isRejected() || $calls !== 1 || $r->cancel(Promise::resolved(1), $cancelAttempt)){
        throw new RuntimeException('Late result settled a cancelled run.');
    }
});

$test('cancelling a run waiting for its retry drops the timer', static function() use ($retrier, $attempts) : void{
    [$r, $advance, $cancelled, , $cancelAttempt] = $retrier();
    $calls = 0;
    $promise = $r->run($attempts([new RpcOverloadedException(1), 'ok'], $calls), new RpcRetryPolicy(baseDelayMs: 1, maxDelayMs: 1, jitter: 0.0));
    if($r->waiting() !== 1 || !$r->cancel($promise, $cancelAttempt) || $r->waiting() !== 0 || count($cancelled) !== 0){
        throw new RuntimeException('Waiting retry was not dropped.');
    }
    $advance(10);
    if($calls !== 1 || !$promise->reason() instanceof CancelledException){
        throw new RuntimeException('Cancelled run retried.');
    }
});

$test('a run cancelled by a callback during tick never sends again', static function() use ($retrier, $attempts) : void{
    [$r, $advance, , , $cancelAttempt] = $retrier();
    $calls = 0;
    $policy = new RpcRetryPolicy(maxAttempts: 2, baseDelayMs: 1, maxDelayMs: 1, jitter: 0.0);
    $second = null;
    $first = $r->run($attempts([new RpcOverloadedException(1), new RpcOverloadedException(1)], $calls), $policy);
    $first->catch(static function() use ($r, $cancelAttempt, &$second) : void{ $r->cancel($second, $cancelAttempt); });
    $second = $r->run($attempts([new RpcOverloadedException(1), 'ok'], $calls), $policy);
    $advance(1);
    if($calls !== 3 || !$second->reason() instanceof CancelledException || $r->waiting() !== 0){
        throw new RuntimeException("Cancelled run kept going: calls=$calls");
    }
});

$test('a throwing observer on the retry promise is reported, not lost', static function() use ($retrier, $attempts) : void{
    [$r, , , $errors] = $retrier();
    $calls = 0;
    $deferreds = new ArrayObject();
    $after = false;
    $promise = $r->run($attempts([null], $calls, $deferreds), new RpcRetryPolicy());
    $promise->observe(static function() : void{ throw new RuntimeException('observer'); });
    $promise->observe(static function() use (&$after) : void{ $after = true; });
    $deferreds[0]->resolve('ok');
    if(!$after || count($errors) !== 1 || $errors[0]->getMessage() !== 'observer' || $promise->value() !== 'ok'){
        throw new RuntimeException('Observer exception escaped or stopped later observers.');
    }
});

$test('a stale retry timer never fires into a newer run that reuses its key', static function() use ($retrier, $attempts) : void{
    [$r, $advance, , , $cancelAttempt] = $retrier();
    $policy = new RpcRetryPolicy(maxAttempts: 2, baseDelayMs: 1, maxDelayMs: 1, jitter: 0.0);
    $oldCalls = 0;
    $newCalls = 0;
    $pendingAttempt = $attempts(array_fill(0, 64, null), $newCalls, new ArrayObject());
    $first = $r->run($attempts([new RpcOverloadedException(1), new RpcOverloadedException(1)], $oldCalls), $policy);
    $old = $r->run($attempts([new RpcOverloadedException(1), 'stale'], $oldCalls), $policy);
    $newer = [];
    $reused = false;
    $spare = new stdClass();
    $first->catch(static function() use ($r, $cancelAttempt, $policy, $pendingAttempt, &$old, &$spare, &$newer, &$reused) : void{
        $oldId = spl_object_id($old);
        $r->cancel($old, $cancelAttempt);
        // freed handles are reused last in first out, so the new deferred takes the spare's and its promise the old one
        $old = null;
        $spare = null;
        for($i = 0; $i < 16 && !$reused; ++$i){
            $newer[] = $promise = $r->run($pendingAttempt, $policy);
            $reused = spl_object_id($promise) === $oldId;
        }
    });
    $advance(1);
    if(!$reused){
        throw new RuntimeException('Could not get a new run onto the cancelled run\'s key.');
    }
    foreach($newer as $promise){
        if(!$promise->isPending()){
            throw new RuntimeException('A stale timer settled a newer run.');
        }
    }
    if($oldCalls !== 3){
        throw new RuntimeException("The cancelled run sent again: calls=$oldCalls");
    }
});

$failures = 0;
foreach($tests as $name => $callback){
    try{
        $callback();
        fwrite(STDOUT, "PASS $name\n");
    }catch(Throwable $e){
        ++$failures;
        fwrite(STDERR, "FAIL $name: {$e->getMessage()}\n");
    }
}
fwrite(STDOUT, count($tests) . " tests, $failures failures\n");
exit($failures === 0 ? 0 : 1);