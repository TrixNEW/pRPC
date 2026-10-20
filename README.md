# pRPC

Asynchronous gRPC client for **PocketMine-MP**. Calls run on worker threads, so database and network-service calls never block the server thread.

## Requirements

- PHP 8.4+ with pmmpthread
- PocketMine-MP API 5.x (Quark 6 renamed the `pocketmine\` namespace, so it needs a port)
- `ext-grpc`
- `google/protobuf ^4.31 || ^5.0`
- `ext-protobuf` (recommended, the pure PHP encoder costs about 10 µs per call on the main thread)

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
| `notSent($e)` | The call was rejected before it was sent, including an `RpcWorkerException` with `sent` false |

### Startup and shutdown

`isReady()` returns true once the channel reports a live connection to the server. While it's idle it starts connecting and returns false, so poll it. `drain($timeoutMs)` blocks until every call and waiting retry has settled, so only use it at startup or shutdown.

`close($gracePeriodMs)` stops new calls, waits up to the grace period for calls in flight and rejects the rest with `RpcClosedException`. A worker still inside a call is left to finish it in the background, because a gRPC wait can't be interrupted from another thread, so `close()` doesn't wait out a call's deadline.

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
- A pool of worker threads pulls calls from a shared queue with one lane per endpoint, so a call never waits behind a worker that's busy with something else. Workers use ext-grpc's `Channel` and `Call` directly, no `grpc/grpc` PHP code runs per call.
- Each call is sent and received in one gRPC batch, see [ext-grpc limits](#ext-grpc-limits) for why. `batchSize` above 1 pipelines that many calls per worker instead, which helps a slow backend but isn't safe on a stock ext-grpc.
- `workers` caps how many threads an endpoint may hold, and the last idle thread is kept for an endpoint that has none, so a slow or dead endpoint can't starve the rest.
- `maxOutstanding` and `maxQueuedBytes` bound each endpoint, and the manager bounds them all together. Excess calls are rejected with `RpcOverloadedException` instead of queueing without limit.
- Deadlines start at `call()`. A worker drops a call that expired while queued without sending it, and gRPC enforces the rest natively.
- `reconnectBackoffMinMs` and `reconnectBackoffMaxMs` also set gRPC's own reconnect backoff, which defaults to waiting up to 120s after a backend restart.
- Request, response and metadata sizes are bounded, and those limits also cap the channel's message limits.
- A throwing callback never affects other calls, and every promise settles exactly once.
- Stats are counters and fixed size histograms, nothing grows per request.

### ext-grpc limits

ext-grpc (checked against 1.74.0) waits for every call on one completion queue shared by the whole process, with no timeout. Both problems below were reproduced on Linux with real ext-grpc and pmmpthread.

- **At most 6 threads may wait on it at once.** A 7th returns early while gRPC still writes into its stack. With 8 workers, pRPC 0.1.0-beta.1 died with `*** stack smashing detected ***`. pRPC now caps calls in flight at 6 across every pool, client and shaded copy of pRPC in the process, so any number of clients is safe, they just share those 6 calls.
- **Every new PHP thread replaces that queue.** A call created before the swap and waited on after it never returns. Sending a call as two batches, as `UnaryCall` does, exposes it for the whole round trip. With 20 threads started during traffic, 0.1.0-beta.1 lost 2 workers, timed out 438 of 615 calls and couldn't exit. pRPC now sends each call as one batch, which leaves only the few microseconds inside ext-grpc between creating the call and starting the wait. That survived the same test with nothing lost, but a thread starting inside those microseconds still strands the call, and at thousands of calls per second on every worker it does happen.

pRPC narrows the rest down:

- It only starts its own threads while no worker is between creating a call and waiting on it, holding new calls back for a few milliseconds if needed.
- PocketMine stops async workers that were idle for 5 minutes and starts new ones on demand. Each pool starts every async worker up front, inside that same pause, and sends them a no-op task every 2 minutes so they're never stopped.
- A worker stuck well past its latest deadline has its calls failed with `RpcWorkerException` and is replaced. Its thread can never be joined, so its `quit()` throws and PocketMine finishes shutting down instead of waiting for the server killer. After 16 stuck threads pRPC stops replacing them.

Threads started by other plugins can't be held back. The complete fix is one queue per thread in ext-grpc, see [Native fix](#native-fix).

### Native fix

In ext-grpc's `src/php/ext/grpc/completion_queue.c` declare `TSRM_TLS grpc_completion_queue *completion_queue;` and in `completion_queue.h` make the extern `extern TSRM_TLS grpc_completion_queue *completion_queue;` (`ZEND_TLS` doesn't work here, it includes `static`). Every thread then creates and waits on its own queue in its own `RINIT`, so neither limit applies.

## Benchmarks

Run them with [tests/bench/bench.php](tests/bench/bench.php): `php tests/bench/bench.php <scenario> clients|manager <reps>`, with `PRPC_TRANSPORT=grpc` against [examples/backend](examples/backend), and `PRPC_WORKERS`, `PRPC_BATCH` and `PRPC_SRC` to compare settings or versions. The server loop runs at 20 TPS with 5 ms of work per tick, so a callback can wait up to 5 ms for the tick to finish. "Before" is 0.1.0-beta.1. Medians of 3 runs unless noted, and runs vary by 10 to 20% from one to the next, so only trust bigger differences.

**Real gRPC** on WSL2 Ubuntu 24.04, 12 cores, PHP 8.3.25 ZTS, ext-grpc 1.74.0, ext-protobuf, against the Go backend in the same VM. Its 0.5 ms delay takes about 1.5 ms there, so a call takes about 2 ms end to end. Both sides pipeline 4 calls per worker here, as 0.1.0-beta.1 did by default.

| Scenario | Before p50 / p99 | After p50 / p99 |
| --- | ---: | ---: |
| 1 endpoint, 200 RPS, 2 workers | 5.1 / 78.5 ms | 5.2 / 35.3 ms |
| same on a manager | 5.2 / 142.9 ms | 5.2 / 42.0 ms |
| 5,000 RPS, 2 workers (2 runs) | 386 / 647 ms | 162 / 288 ms |
| 5,000 RPS, 4 workers (2 runs) | 19 / 204 ms | 18 / 80 ms |
| 5,000 RPS, 6 workers (2 runs) | 488 / 640 ms | 14 / 103 ms |
| 20,000 call burst, 6 workers | 6.7k RPS | 8.4k RPS |
| 8 endpoints at 400 RPS on a manager, worst p99 | 62.6 ms | 14.8 ms |
| 8 clients with 1 worker each, worst p99 | crashed, stack smashing | 5.6 / 134 ms, no errors |

A healthy endpoint at 100 RPS on a 4 worker manager next to a bad one, p99 of the healthy one:

| Next to | Before | After |
| --- | ---: | ---: |
| slow, 200 ms | 165.8 ms | 55.3 ms |
| blackhole, calls time out at 1.5 s | 125.5 ms | 33.3 ms |
| down, fails fast | 76.8 ms | 59.0 ms |
| flapping every 500 ms | 72.3 ms | 39.2 ms |

With one call per worker, the default, throughput is about workers divided by the round trip. On this backend 6 workers took about 4.5k RPS at 5,000 offered (p50 346 ms, so already queueing) and about 4.2k RPS in the burst, against 8.4k pipelined. That's the price of not hanging when a thread starts. Raise `workers` first, up to 6, and only raise `batchSize` on a build with the [native fix](#native-fix).

**Fake transport** on Windows 11, 12 cores, PHP 8.4 ZTS with pmmpthread and no ext-protobuf. [It](tests/bench/fake) keeps ext-grpc's blocking model but stands in for the network, so it measures pRPC's own dispatch, queueing and threading. Both sides pipeline 4 calls per worker.

| Scenario | Before p50 / p99 | After p50 / p99 |
| --- | ---: | ---: |
| 1 endpoint, 200 RPS | 5.2 / 5.5 ms | 5.2 / 5.6 ms |
| 1 endpoint, 5,000 RPS | 8.7 / 13.1 ms | 9.0 / 13.2 ms |
| 20,000 call burst on a 4 worker manager, 9 runs interleaved | 40.8k RPS | 43.0k RPS |
| main thread per call at 5,000 RPS, 4 runs interleaved | 26 µs | 24 µs |

Nothing to see there, which is the point: dispatch cost didn't change, the work went into not hanging, crashing or leaking. Idle CPU stays at zero, workers sleep on a condition variable. About 11 µs of the main thread's 24 µs per call is pure PHP protobuf encoding and decoding, which ext-protobuf removes.

ext-grpc's completion queue, threads started during traffic on a backend that answers at once, 15 runs each:

| Case | Stock 1.74.0 | With the native fix |
| --- | ---: | ---: |
| send and receive as two batches, 4 threads | 15 hung | 0 |
| one batch, 6 threads | 7 hung | 0 |
| two batches, 6 threads | 14 hung | 0 |
| one batch, 8 threads | 15 crashed | 0 |
| two batches, 8 threads | 15 crashed | 0 |

With every thread started before the first call, stock ext-grpc hung 0 of 15 times in both modes, so the swap is the whole problem.

## Tests

`composer test` runs the unit tests on any PHP.

`php tests/threads.php` runs real worker threads against [the fake transport](tests/bench/fake) and needs pmmpthread plus a PocketMine-MP checkout in `PRPC_PM`. It covers 2 to 16 endpoints, slow, dead, flapping and hung endpoints, fairness, limits, timeouts, crashed and stuck workers, the 6 call cap across pools, replacement while other calls are in flight, endpoints added and removed under traffic, oversized and undecodable responses, 20,000 pending calls, 100,000 calls with flat memory (`PRPC_LONG_ROUNDS` scales it), shutdown and repeated creation.

`php tests/grpc.php` runs real worker threads against ext-grpc and [examples/backend](examples/backend). It covers round trips in both call modes, threads started mid call, more than 6 workers across clients, and down, blackhole and flapping backends.

## Production checklist

- Enable `ext-grpc`, and `ext-protobuf` for the lowest overhead.
- Use a PHP build with the [native fix](#native-fix) if you can.
- Keep `batchSize` at 1 unless that build has the fix.
- Use TLS or mTLS for traffic over an untrusted network.
- Keep deadlines short so shutdown and failure recovery stay bounded.
- Size the request, response, metadata, outstanding and queued bytes limits from real payloads.
- Call `close()` when the plugin disables.
- Load test against the real server and backend before deploying.

## License

Apache-2.0
