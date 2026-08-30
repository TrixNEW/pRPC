<?php

declare(strict_types=1);

require dirname(__DIR__) . '/vendor/autoload.php';

use Google\Protobuf\Internal\Message;
use Trix\pRPC\opts\RpcCallOptions;
use Trix\pRPC\opts\RpcClientConfig;
use Trix\pRPC\RpcMethod;
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