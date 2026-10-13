# pRPC

Asynchronous gRPC client for **PocketMine-MP / Quark**. Calls run on dedicated worker threads, so database and network-service calls never block the server thread.

## Requirements

- PHP 8.4+
- PocketMine-MP / Quark API 5.x
- `ext-grpc`
- `grpc/grpc ^1.82`
- `google/protobuf ^4.31 || ^5.0`
- `ext-protobuf` (recommended for performance)

```bash
composer install
composer test
```

pRPC handles unary calls only. Long-lived server or bidirectional streams need a separate transport.

## Usage

Define the methods your service exposes:

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
use Trix\pRPC\opts\RpcClientConfig;
use Trix\pRPC\opts\RpcCredentials;

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

Make a call. Callbacks run on the main thread:

```php
$this->rpc->call($getPlayer, $request)
    ->then(function(GetPlayerResponse $response) : void{
        // ...
    })
    ->onError(function(Throwable $error) : void{
        // timeout, overload, worker, gRPC or decode failure
    });
```

Per-call timeout and metadata:

```php
use Trix\pRPC\opts\RpcCallOptions;

$this->rpc->call($getPlayer, $request, new RpcCallOptions(
    timeoutMs: 1000,
    metadata: ['server-id' => 'factions-1']
));
```

Metadata keys are lowercased, and keys that collide after lowercasing are rejected.

Close the client when the plugin disables:

```php
$this->rpc->close();
```

### Retries

```php
use Trix\pRPC\retry\RpcRetryPolicy;

$this->rpc->callRetrying($method, $request, new RpcRetryPolicy(maxAttempts: 5, baseDelayMs: 100));
```

Retries use exponential backoff with jitter and resend the same request object, so put any idempotency key inside it.

Timeouts and `UNAVAILABLE` are ambiguous because the server may already have applied the request. They are only retried with `retryAmbiguous: true`, which should only be set when the server deduplicates requests. `INTERNAL`, `UNKNOWN` and `CANCELLED` are also ambiguous but usually final, so they get at most `maxUncertainAttempts` tries.

`RpcStatus` classifies a failure:

| Method | True when |
| --- | --- |
| `isRetryable($e)` | Sending again can succeed |
| `isAmbiguous($e)` | The server may have applied the request |
| `notSent($e)` | The call was rejected before it was sent |

### Startup and shutdown

`isReady()` returns true once a worker has a connected channel. `drain($timeoutMs)` blocks until every call and waiting retry has settled, so only use it at startup or shutdown.

## API

| Class | Purpose |
| --- | --- |
| `RpcClient` | Dispatches calls and manages workers |
| `RpcClientConfig` | Endpoint, workers, timeouts, limits and channel options |
| `RpcMethod` | Maps a gRPC path to its request and response classes |
| `RpcCallOptions` | Per-call timeout and metadata |
| `RpcCredentials` | Insecure, TLS or mTLS credentials |
| `RpcPromise` | `then()`, `onError()` and `always()` callbacks |
| `RpcRetryPolicy` | Attempts, backoff and jitter for `callRetrying()` |
| `RpcStatus` | Failure classification |
| `RpcClientStats` | Pending calls, transport load, worker states and waiting retries, from `$rpc->stats()` |

## Design

- No network I/O on the main thread.
- Requests are encoded once and responses decoded once, on the main thread.
- `maxOutstanding` bounds the queue during backend outages.
- Native and local deadlines keep calls from pending forever.
- Request, response and metadata sizes are bounded, and those limits also cap the channel's message limits.
- A throwing callback never affects other calls.
- Worker wakeups are coalesced and reconnects back off exponentially.

For a local or LAN service, start with 2 to 4 workers and batch size 4, then benchmark against the real backend before raising them.

## Benchmarks

Unary GetPlayer calls against a gRPC backend on 127.0.0.1, measured from request creation to the main-thread callback. Every call succeeded. Results depend on CPU, PHP build, payload size and backend work.

| Calls | Workers | Batch | Time | Throughput | Avg latency | p95 | p99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 2 | 2 | 0.182 s | 5,492 RPS | 59.85 ms | 113.02 ms | 144.24 ms |
| 5,000 | 4 | 4 | 0.367 s | 13,621 RPS | 209.74 ms | 253.95 ms | 287.06 ms |
| 6,000 | 6 | 4 | 0.519 s | 11,549 RPS | 375.40 ms | 420.34 ms | 454.98 ms |
| 20,000 | 6 | 16 | 0.677 s | 29,571 RPS | 261.75 ms | 398.86 ms | 432.83 ms |

The 20,000 call row is the average of three runs. See [tests/benchmark_prpc.php](tests/benchmark_prpc.php) and [tests/test_extreme_rps.php](tests/test_extreme_rps.php).

## Production checklist

- Enable `ext-grpc`, and `ext-protobuf` for the lowest overhead.
- Use TLS or mTLS for traffic over an untrusted network.
- Keep deadlines short so shutdown and failure recovery stay bounded.
- Size the request, response, metadata and outstanding limits from real payloads.
- Call `close()` when the plugin disables.
- Load test against the real server and backend before deploying.

## License

Apache-2.0
