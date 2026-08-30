# pRPC

Fast, asynchronous gRPC transport for **PocketMine-MP / Quark**, designed for low-latency database and network-service calls without blocking the server thread.

pRPC uses dedicated worker threads, bounded backpressure, native gRPC deadlines, compact cross-thread frames, and a raw protobuf transport path to minimize allocations and duplicate serialization work.

## Requirements

- **PHP 8.4+**
- **PocketMine-MP / Quark API 5.x**
- **ext-grpc**
- `grpc/grpc ^1.82`
- `google/protobuf ^4.31 || ^5.0`
- **ext-protobuf** *(strongly recommended for best protobuf performance)*

Install PHP dependencies with Composer:

```bash
composer install
```

Run the regression suite with:

```bash
composer test
```

> `ext-grpc` must be installed in the PHP build used to run the server. pRPC is intended for unary request/response RPCs; long-lived server/bidi streams should use a separate dedicated stream transport.

## Quick Start

Define the RPC methods your service exposes:

```php
use Trix\pRPC\RpcMethod;

$getPlayer = new RpcMethod(
    '/network.DataService/GetPlayer',
    GetPlayerRequest::class,
    GetPlayerResponse::class
);
```

Create one shared client for your plugin:

```php
use Trix\pRPC\RpcClient;
use Trix\pRPC\RpcClientConfig;
use Trix\pRPC\RpcCredentials;

$this->rpc = new RpcClient($this, new RpcClientConfig(
    endpoint: '127.0.0.1:50051',
    methods: [$getPlayer],
    credentials: RpcCredentials::insecure(),
    workers: 4,
    batchSize: 4,
    defaultTimeoutMs: 1500,
    maxTimeoutMs: 5000,
    maxOutstanding: 4096
));
```

Call it asynchronously:

```php
$this->rpc->call($getPlayer, $request)
    ->then(function(GetPlayerResponse $response) : void{
        // Runs on the main thread.
    })
    ->onError(function(Throwable $error) : void{
        // Timeout, overload, worker, gRPC, or decode failure.
    });
```

Optional per-call timeout and metadata:

```php
use Trix\pRPC\RpcCallOptions;

$this->rpc->call($getPlayer, $request, new RpcCallOptions(
    timeoutMs: 1000,
    metadata: [
        'server-id' => 'factions-1',
        'authorization' => 'Bearer secret'
    ]
));
```

Metadata keys are case-insensitive and normalized to lowercase. Supplying keys that collide after normalization, such as `Authorization` and `authorization`, is rejected.

Close the client when the plugin disables:

```php
$this->rpc->close();
```

## API

| Class | Purpose |
| --- | --- |
| `RpcClient` | Dispatches asynchronous unary calls and manages workers |
| `RpcClientConfig` | Endpoint, workers, timeouts, limits and channel options |
| `RpcMethod` | Maps a gRPC path to request/response protobuf classes |
| `RpcCallOptions` | Per-call timeout and metadata |
| `RpcCredentials` | Insecure, TLS or mTLS channel credentials |
| `RpcPromise` | `then()`, `onError()` and `always()` callbacks |
| `RpcClientStats` | Pending calls, transport load and worker states |

Runtime stats are available through:

```php
$stats = $this->rpc->stats();
```

## Performance & Safety

- No network I/O on the PocketMine main thread.
- Protobuf requests are encoded once and responses decoded once on the main thread.
- Bounded `maxOutstanding` prevents unlimited queue growth during backend outages.
- Worker selection avoids per-request cross-thread load polling.
- Native + local deadlines prevent indefinitely pending RPCs.
- Request, response and metadata sizes are bounded.
- Configured request and response limits also cap the corresponding gRPC channel message limits, preventing larger channel overrides from bypassing allocation bounds.
- Promise callback exceptions are isolated from unrelated RPC completions.
- Worker result notifications are coalesced while results are waiting, reducing unnecessary main-thread wakeups.
- Worker reconnects use exponential backoff.

For a local/LAN database service, the defaults are intentionally latency-oriented. Start around **2–4 workers** and **batch size 4**, then benchmark against the real backend before increasing concurrency.

## Benchmarks

These burst tests use PocketMine runtime components, the pRPC client and a unary GetPlayer call against a gRPC backend on 127.0.0.1:50051. Measurements include request creation and protobuf encoding, worker queueing, gRPC request/response handling, protobuf response decoding and main-thread completion callbacks. Every measured call completed successfully.

These are standalone transport benchmarks rather than a live gameplay-server test. Results vary with CPU, PHP build and extensions, payload size, backend work and network placement.

### Standard burst tests

| Calls | Workers | Batch | Total time | Throughput | Average latency | p95 | p99 | Errors |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 2 | 2 | 0.182 s | 5,492.1 RPS | 59.85 ms | 113.02 ms | 144.24 ms | 0 |
| 5,000 | 4 | 4 | 0.367 s | 13,620.9 RPS | 209.74 ms | 253.95 ms | 287.06 ms | 0 |
| 6,000 | 6 | 4 | 0.519 s | 11,549.3 RPS | 375.40 ms | 420.34 ms | 454.98 ms | 0 |

The four-worker configuration delivered the best standard-run throughput at **13,620.9 completed calls per second**.

### Extreme burst test

All three runs used 20,000 calls, 6 workers and batch size 16.

| Run | Total time | Throughput | Average latency | p95 | p99 | Errors |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.654 s | 30,567.8 RPS | 247.04 ms | 391.54 ms | 428.20 ms | 0 |
| 2 | 0.681 s | 29,378.9 RPS | 260.99 ms | 412.10 ms | 444.61 ms | 0 |
| 3 | 0.695 s | 28,767.1 RPS | 277.23 ms | 392.94 ms | 425.69 ms | 0 |

Across the three extreme runs, pRPC averaged **29,571.3 RPS**, **261.75 ms average latency**, **398.86 ms p95** and **432.83 ms p99**, with **60,000 of 60,000 calls completed and zero errors**.

The standard suite is in [tests/benchmark_prpc.php](tests/benchmark_prpc.php), and the stress suite is in [tests/test_extreme_rps.php](tests/test_extreme_rps.php).

## Production Checklist

- Install and enable `ext-grpc`; enable `ext-protobuf` for the lowest protobuf overhead.
- Use TLS or mTLS credentials for traffic that crosses an untrusted network.
- Keep deadlines short and appropriate for the backend so shutdown and failure recovery remain bounded.
- Set request, response, metadata and outstanding-call limits from measured production payloads and capacity.
- Call `close()` when the plugin disables.
- Run load and soak tests against the real PocketMine build and gRPC service before deployment.

## License

Apache-2.0
