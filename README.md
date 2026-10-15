# pRPC

Asynchronous gRPC client for **PocketMine-MP / Quark**. Calls run on worker threads, so database and network-service calls never block the server thread.

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

### Several endpoints

`RpcClientManager` runs every endpoint on one shared set of worker threads instead of a set per client. Each endpoint keeps its own methods, credentials, timeouts and limits.

```php
use Trix\pRPC\RpcClientManager;

$this->rpc = new RpcClientManager($this, workers: 4);

$this->rpc->register('asgard', new RpcClientConfig(
    endpoint: '10.0.0.5:50051',
    methods: Methods::all(),
    workers: 3
));
$this->rpc->register('matchmaker', new RpcClientConfig(
    endpoint: 'mm.internal:443',
    methods: [$findMatch],
    credentials: RpcCredentials::tlsFromFiles('/etc/ca.pem'),
    workers: 1,
    defaultTimeoutMs: 3000
));

$this->rpc->get('asgard')->call($getPlayer, $request)->then(...);
```

On a manager, `workers` in `RpcClientConfig` caps how many of the shared threads that endpoint may hold at once, so a slow or dead endpoint can't take them all. `get()` throws for a name that isn't registered and a client rejects methods it wasn't given. `remove('matchmaker')` closes one endpoint and leaves the rest running, and `close()` closes everything. Endpoints with the same address, credentials and channel options share one gRPC channel.

Calls are never moved to another endpoint, each name is its own service.

### Retries

```php
use Trix\pRPC\retry\RpcRetryPolicy;

$this->rpc->callRetrying($method, $request, new RpcRetryPolicy(maxAttempts: 5, baseDelayMs: 100));
```

Retries use exponential backoff with jitter and resend the same request object, so put any idempotency key inside it. `totalTimeoutMs` stops retrying once the next attempt would start past that budget.

Timeouts and `UNAVAILABLE` are ambiguous because the server may already have applied the request. They are only retried with `retryAmbiguous: true`, which should only be set when the server deduplicates requests. `INTERNAL`, `UNKNOWN` and `CANCELLED` are also ambiguous but usually final, so they get at most `maxUncertainAttempts` tries.

`RpcStatus` classifies a failure:

| Method | True when |
| --- | --- |
| `isRetryable($e)` | Sending again can succeed |
| `isAmbiguous($e)` | The server may have applied the request |
| `notSent($e)` | The call was rejected before it was sent |

### Startup and shutdown

`isReady()` returns true once the channel reports a live connection to the server. While it's idle it starts connecting and returns false, so poll it. `drain($timeoutMs)` blocks until every call and waiting retry has settled, so only use it at startup or shutdown.

`close($gracePeriodMs)` stops new calls, waits up to the grace period for calls in flight and rejects the rest with `RpcClosedException`. A worker still inside a call is left to finish it in the background, because `UnaryCall::wait()` can't be interrupted from another thread, so `close()` doesn't wait out a call's deadline.

## API

| Class | Purpose |
| --- | --- |
| `RpcClient` | Calls one endpoint, on its own workers or a manager's |
| `RpcClientManager` | Named endpoints on one shared set of workers |
| `RpcClientConfig` | Endpoint, workers, timeouts, limits and channel options |
| `RpcMethod` | Maps a gRPC path to its request and response classes |
| `RpcCallOptions` | Per-call timeout and metadata |
| `RpcCredentials` | Insecure, TLS or mTLS credentials |
| `RpcPromise` | `then()`, `onError()` and `always()` callbacks |
| `RpcRetryPolicy` | Attempts, backoff and jitter for `callRetrying()` |
| `RpcStatus` | Failure classification |
| `RpcClientStats` | Per endpoint state, queue, counters and latency, from `$rpc->stats()` or `$manager->stats()` |
| `RpcLatency` | Call and queue wait histograms with `percentileMs()` |

## Design

- No network I/O on the main thread. Requests are encoded once and responses decoded once, on the main thread, and callbacks always run there.
- A pool of worker threads pulls calls from a shared queue with one lane per endpoint, so a call never waits behind a worker that's busy with something else. A woken worker takes up to `batchSize` calls from one endpoint, starts them all on the shared HTTP/2 channel and then waits on each.
- `workers` caps how many threads an endpoint may hold, and the last idle thread is kept for an endpoint that has none, so a slow or dead endpoint can't starve the rest.
- `maxOutstanding` and `maxQueuedBytes` bound each endpoint, and the manager bounds them all together. Excess calls are rejected with `RpcOverloadedException` instead of queueing without limit.
- Deadlines start at `call()`. A worker drops a call that expired while queued without sending it, and gRPC enforces the rest natively.
- `reconnectBackoffMinMs` and `reconnectBackoffMaxMs` also set gRPC's own reconnect backoff, which defaults to waiting up to 120s after a backend restart.
- Request, response and metadata sizes are bounded, and those limits also cap the channel's message limits.
- A throwing callback never affects other calls, and every promise settles exactly once.
- Stats are counters and fixed size histograms, nothing grows per request.

### ext-grpc limits

ext-grpc waits for every call on one completion queue shared by the whole process, with an infinite timeout. Two things follow, and pRPC works around both:

- At most 6 threads may wait on it at once. A 7th returns early while gRPC is still writing into its stack, so a pool never runs more than 6 workers. Keep the total across every pool and plugin at 6 or below, one `RpcClientManager` per plugin is the easy way.
- Every new PHP thread, including AsyncPool workers PocketMine restarts during play, replaces that queue. A call started just before can then wait forever. pRPC notices a worker that is well past its latest deadline, fails its calls with `RpcWorkerException`, replaces it and never joins it.

Both go away if ext-grpc keeps one queue per thread. That's a two line change, untested here: in `src/php/ext/grpc/completion_queue.c` declare `ZEND_TLS grpc_completion_queue *completion_queue;` and in `completion_queue.h` make the extern `extern ZEND_TLS grpc_completion_queue *completion_queue;`.

## Benchmarks

Measured on Windows 11, 12 cores, PHP 8.4.16 ZTS with pmmpthread and no ext-protobuf, using real worker threads and [the fake transport](tests/bench/fake), which keeps ext-grpc's blocking model but stands in for the network. They compare the old and new dispatch, queueing and threading, not gRPC itself. The server loop runs at 20 TPS with 5 ms of work per tick, so a callback can wait up to 5 ms for the tick to finish. Median of 3 runs.

| Scenario | Before p50 / p99 | After p50 / p99 |
| --- | ---: | ---: |
| 1 endpoint, 200 RPS, 0.5 ms backend | 61.5 / 205.3 ms | 5.3 / 5.6 ms |
| 1 endpoint, 5,000 RPS | 61.7 / 353.6 ms | 9.6 / 12.5 ms |
| 20,000 call burst, 6 workers, batch 16 | 275 / 507 ms, 36.0k RPS | 292 / 476 ms, 32.3k RPS |
| same burst on a 4 worker manager | | 263 / 373 ms, 39.7k RPS |

Most of the old latency came from a bug: results were drained by iterating a `ThreadSafeArray` while unsetting from it, which skips every other entry, so callbacks waited for the next tick.

A healthy endpoint (0.5 ms, 100 RPS) next to a bad one, p99 of the healthy one:

| Next to | Before (separate clients) | After (4 worker manager) |
| --- | ---: | ---: |
| slow, 200 ms | 62.8 ms | 6.2 ms |
| blackhole, calls time out at 1.5 s | 63.4 ms | 5.7 ms |
| down, fails fast | 63.5 ms | 6.0 ms |
| flapping every 500 ms | 62.5 ms | 5.7 ms |

Many endpoints at 400 RPS in total, worst endpoint p99, threads and process RSS:

| Endpoints | Before | After (4 worker manager) |
| ---: | ---: | ---: |
| 4 | 211.2 ms, 4 threads, 33.4 MB | 6.0 ms, 4 threads, 33.6 MB |
| 8 | 62.4 ms, 8 threads, 36.5 MB | 6.1 ms, 4 threads, 33.6 MB |
| 16 | 63.0 ms, 16 threads, 43.0 MB | 7.2 ms, 4 threads, 33.7 MB |

Idle CPU stays at zero in both, workers sleep on a condition variable. The main thread spends about 25 µs per call before and 30 µs after at 5,000 RPS, since results are now handled as they arrive instead of once per tick.

Run them with [tests/bench/bench.php](tests/bench/bench.php), and against a real backend with `PRPC_TRANSPORT=grpc` and [examples/backend](examples/backend).

## Tests

`composer test` runs the unit tests on any PHP. `php tests/threads.php` runs real worker threads against the fake transport and needs pmmpthread plus a PocketMine-MP checkout in `PRPC_PM`. It covers 2 to 16 endpoints, slow, dead, flapping and hung endpoints, fairness, limits, timeouts, crashed and stuck workers, shutdown and repeated creation.

## Production checklist

- Enable `ext-grpc`, and `ext-protobuf` for the lowest overhead.
- Keep pRPC at 6 worker threads or fewer in the whole process, see [ext-grpc limits](#ext-grpc-limits).
- Use TLS or mTLS for traffic over an untrusted network.
- Keep deadlines short so shutdown and failure recovery stay bounded.
- Size the request, response, metadata, outstanding and queued bytes limits from real payloads.
- Call `close()` when the plugin disables.
- Load test against the real server and backend before deploying.

## License

Apache-2.0
