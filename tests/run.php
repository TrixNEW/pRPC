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
use Trix\pRPC\utils\promise\RpcPromise;
use Trix\pRPC\utils\promise\RpcPromiseResolver;

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

$test('promise settlement clears captured callback objects', static function() : void{
    $errors = [];
    $resolver = new RpcPromiseResolver(static function(Throwable $e) use (&$errors) : void{
        $errors[] = $e;
    });
    $captured = new stdClass();
    $reference = WeakReference::create($captured);
    $value = null;
    $resolver->promise()->then(static function(string $result) use (&$value, $captured) : void{
        $value = $result;
    });
    unset($captured);
    $resolver->resolve('ok');
    gc_collect_cycles();
    if($value !== 'ok' || $reference->get() !== null || $errors !== []){
        throw new RuntimeException('Promise callback retention or settlement failure.');
    }
});

$test('callback exceptions are isolated', static function() : void{
    $errors = [];
    $resolver = new RpcPromiseResolver(static function(Throwable $e) use (&$errors) : void{
        $errors[] = $e->getMessage();
    });
    $alwaysRan = false;
    $resolver->promise()
        ->then(static function() : void{ throw new RuntimeException('callback failed'); })
        ->always(static function() use (&$alwaysRan) : void{ $alwaysRan = true; });
    $resolver->resolve('ok');
    if($errors !== ['callback failed'] || !$alwaysRan){
        throw new RuntimeException('Callback failure was not isolated.');
    }
});

$test('failures are classified by whether a retry is safe', static function() : void{
    $cases = [
        [new RpcOverloadedException(10), true, false],
        [new RpcMessageTooLargeException('request', 10, 5), false, false],
        [new RpcTimeoutException(100), true, true],
        [new RpcClosedException(), false, true],
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

/** @return array{RpcRetrier, Closure(int): void} */
$retrier = static function() : array{
    $now = 0;
    $r = new RpcRetrier(static function(Throwable $e) : void{ throw $e; }, static function() use (&$now) : int{ return $now; });
    return [$r, static function(int $ms) use (&$now, $r) : void{ $now += $ms * 1_000_000; $r->tick(); }];
};

$attempts = static function(array $outcomes, int &$calls) : Closure{
    return static function() use (&$outcomes, &$calls) : RpcPromise{
        ++$calls;
        $resolver = new RpcPromiseResolver(static function(Throwable $e) : void{ throw $e; });
        $next = array_shift($outcomes);
        if($next instanceof Throwable){
            $resolver->reject($next);
        }else{
            $resolver->resolve($next);
        }
        return $resolver->promise();
    };
};

$test('retrier resends transient failures after the delay and resolves once', static function() use ($retrier, $attempts) : void{
    [$r, $advance] = $retrier();
    $calls = 0;
    $result = null;
    $retries = [];
    $r->run(
        $attempts([new RpcTimeoutException(5), new RpcException('down', RpcStatus::UNAVAILABLE), 'ok'], $calls),
        new RpcRetryPolicy(maxAttempts: 5, baseDelayMs: 10, maxDelayMs: 100, jitter: 0.0),
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
        ->onError(static function(Throwable $e) use (&$error) : void{ $error = $e; });
    if($calls !== 1 || $error === null || $r->waiting() !== 0){
        throw new RuntimeException('Final error was retried.');
    }

    $calls = 0;
    $error = null;
    $r->run($attempts(array_fill(0, 5, new RpcOverloadedException(1)), $calls), new RpcRetryPolicy(maxAttempts: 3, baseDelayMs: 1, maxDelayMs: 1, jitter: 0.0))
        ->onError(static function(Throwable $e) use (&$error) : void{ $error = $e; });
    $advance(1);
    $advance(1);
    $advance(1);
    if($calls !== 3 || !$error instanceof RpcOverloadedException){
        throw new RuntimeException("Attempt limit ignored: calls=$calls");
    }

    $calls = 0;
    $error = null;
    $r->run($attempts([new RpcTimeoutException(1), 'ok'], $calls), new RpcRetryPolicy(retryAmbiguous: false))
        ->onError(static function(Throwable $e) use (&$error) : void{ $error = $e; });
    if($calls !== 1 || !$error instanceof RpcTimeoutException){
        throw new RuntimeException('Ambiguous failure was retried.');
    }
});

$test('stopping the retrier runs waiting retries immediately', static function() use ($retrier, $attempts) : void{
    [$r] = $retrier();
    $calls = 0;
    $error = null;
    $r->run($attempts([new RpcOverloadedException(1), new RpcClosedException()], $calls), new RpcRetryPolicy(baseDelayMs: 1_000, maxDelayMs: 1_000))
        ->onError(static function(Throwable $e) use (&$error) : void{ $error = $e; });
    $r->stop();
    if($calls !== 2 || !$error instanceof RpcClosedException || $r->waiting() !== 0){
        throw new RuntimeException('Stop did not settle waiting retries.');
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