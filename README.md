# pRPC

High-performance asynchronous gRPC client for **PocketMine-MP**, designed for low-overhead communication with databases and external services.

pRPC executes gRPC calls on worker threads, keeping network operations off the main server thread while delivering responses through callbacks.

## Features

- **Asynchronous RPC** — Non-blocking unary calls using dedicated worker threads.
- **Shared worker pools** — Multiple endpoints can share workers with independent concurrency limits.
- **Connection management** — Channel reuse, reconnection, deadlines, and graceful shutdown.
- **Reliable execution** — Bounded queues, overload protection, failure handling, and configurable retries.
- **Low overhead** — Direct `ext-grpc` integration with optional request pipelining.
- **Monitoring** — Per-endpoint statistics, latency histograms, and queue metrics.
- **Security** — TLS, mTLS, metadata, and configurable message-size limits.

## Requirements

- PHP 8.4+ with `pmmpthread`
- PocketMine-MP API 5.x
- `ext-grpc`
- `google/protobuf ^4.31 || ^5.0`
- `ext-protobuf` (recommended for optimal performance)

> **Recommended:** Use [TrixNEW/ext-grpc](https://github.com/TrixNEW/ext-grpc), a patched version of ext-grpc 1.74.0 that fixes completion queue issues affecting multithreaded PHP applications.
>
> The fork provides per-thread completion queues, avoiding the shared queue's six-call limit and thread-related hangs or crashes. It also enables higher concurrency and safe request pipelining in pRPC. Stock ext-grpc remains supported with additional safeguards and reduced concurrency.

**Compatibility:** Quark 6 requires a namespace migration before pRPC can be used directly.

## Installation

```bash
composer install
composer test
```

## Usage

Define a method and initialize a shared RPC client:

```php
use Trix\pRPC\RpcClient;
use Trix\pRPC\RpcMethod;
use Trix\pRPC\opts\RpcClientConfig;
use Trix\pRPC\opts\RpcCredentials;

$getPlayer = new RpcMethod(
    '/network.DataService/GetPlayer',
    GetPlayerRequest::class,
    GetPlayerResponse::class
);

$this->rpc = new RpcClient($this, new RpcClientConfig(
    endpoint: '127.0.0.1:50051',
    methods: [$getPlayer],
    credentials: RpcCredentials::insecure(),
    workers: 4,
    defaultTimeoutMs: 1500
));
```

Execute an asynchronous call:

```php
$this->rpc->call($getPlayer, $request)
    ->then(function(GetPlayerResponse $response): void {
        // Handle response
    })
    ->onError(function(Throwable $error): void {
        // Handle failure
    });
```

Callbacks execute on the main thread. Close the client when disabling your plugin:

```php
$this->rpc->close();
```

### Multiple endpoints

`RpcClientManager` allows multiple services to share a worker pool while maintaining independent configurations and concurrency limits.

```php
use Trix\pRPC\RpcClientManager;

$this->rpc = new RpcClientManager($this, workers: 4);

$this->rpc->register('asgard', new RpcClientConfig(
    endpoint: '127.0.0.1:50051',
    methods: Methods::all(),
    workers: 3
));

$this->rpc->get('asgard')->call($getPlayer, $request);
```

### Retries

pRPC supports configurable retries with exponential backoff and jitter through `RpcRetryPolicy`.

```php
use Trix\pRPC\retry\RpcRetryPolicy;

$this->rpc->callRetrying(
    $getPlayer,
    $request,
    new RpcRetryPolicy(maxAttempts: 5, baseDelayMs: 100)
);
```

Ambiguous failures are not retried by default. Enable ambiguous retries only for requests protected by server-side deduplication.

## Performance

pRPC uses direct `ext-grpc` calls, bounded queues, and reusable worker threads to minimize overhead and maintain predictable resource usage.

For higher concurrency, the [patched ext-grpc](https://github.com/TrixNEW/ext-grpc) is strongly recommended. Stock ext-grpc has known multithreading limitations, and request pipelining should remain disabled without the native fix.

Benchmarks and stress tests are available in [tests/bench](tests/bench).

## Testing

```bash
composer test
php tests/threads.php
php tests/grpc.php
```

Threaded and integration tests require the appropriate PocketMine-MP, pmmpthread, and gRPC environments.

## License

Licensed under the Apache-2.0 License.
