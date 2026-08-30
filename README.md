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

## Estimated Comparison with libasynql

> **These are engineering estimates, not measured benchmark results.** libasynql executes SQL directly, while pRPC calls a separate gRPC service. Results depend primarily on network latency, query cost, service caching and how much work one RPC replaces. Do not cite these ranges as benchmarks.

Assume PocketMine and the backend run on the same machine or low-latency LAN, connections are warm, protobuf payloads are under 1 KiB and the database is not saturated:

| Workload | Expected pRPC result relative to libasynql | Why |
| --- | --- | --- |
| One trivial, uncached SQL query proxied unchanged | About **10–30% slower** (0.7–0.9x) | pRPC adds protobuf, gRPC and a service hop on top of the same database query. |
| Cached read served by the RPC service | Roughly **2–4x faster** (100–300% higher effective operation rate) | The service can avoid the SQL round trip; this is a caching advantage, not an inherent transport advantage. |
| One RPC replacing 3–10 dependent SQL operations | Roughly **1.5–3x faster** (50–200%) | Validation, transactions and intermediate work stay beside the database instead of crossing the PocketMine boundary repeatedly. |
| High concurrency against the same saturated database | Usually **similar throughput** | Database capacity becomes the bottleneck; changing the asynchronous transport cannot create database capacity. |

For a simple direct query, libasynql is the appropriate latency baseline and may be faster. pRPC is intended for service-oriented backends where one typed call can apply caching, authorization, validation, batching or multiple database operations. Publish concrete numbers only after running both libraries against the same schema, payloads, concurrency, hardware and backend state, reporting at least throughput plus p50, p95 and p99 latency.

## Production Checklist

- Install and enable `ext-grpc`; enable `ext-protobuf` for the lowest protobuf overhead.
- Use TLS or mTLS credentials for traffic that crosses an untrusted network.
- Keep deadlines short and appropriate for the backend so shutdown and failure recovery remain bounded.
- Set request, response, metadata and outstanding-call limits from measured production payloads and capacity.
- Call `close()` when the plugin disables.
- Run load and soak tests against the real PocketMine build and gRPC service before deployment.

## License

Apache-2.0
