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

> **These are engineering estimates, not measured benchmark results.** They describe pRPC's intended architecture: a purpose-built data service with warm persistent connections, compact protobuf responses and optional caching or operation consolidation. Do not cite these ranges as measured benchmarks. **(it's probably much faster then these benchmarks.. lol)**

libasynql sends generic SQL jobs to PocketMine worker threads and returns generic result structures. pRPC sends a compact typed protobuf message to a dedicated service, where database access, validation, authorization, caching and multi-step operations can remain close to the data. This can substantially reduce PocketMine-side encoding, result hydration, cross-thread work and repeated round trips.

Assume PocketMine and the data service run on the same machine or a low-latency LAN, connections are warm, protobuf payloads are under 1 KiB and the backend is not saturated:

| Workload or metric | Expected pRPC result relative to libasynql | Why |
| --- | --- | --- |
| PocketMine-side dispatch and completion overhead | Roughly **2–8x lower overhead** | pRPC uses compact typed frames, one request encoding, one response decoding and dedicated long-lived workers instead of generic SQL parameters and row arrays. |
| Cached player or service-data read | Roughly **3–20x faster** | The data service can answer from memory and avoid executing SQL for every request. |
| One RPC replacing 3–10 dependent SQL operations | Roughly **2–5x faster** | Intermediate work and transactions stay inside the data service instead of crossing the PocketMine boundary for each operation. |
| Compact typed response versus a larger generic SQL row set | Roughly **1.5–4x lower serialization and transfer cost** | Protobuf transmits only the defined fields without generic associative-array keys and structures. |
| One trivial, uncached SQL query proxied unchanged | Usually **similar**, and potentially **up to 30% slower** | If the service adds no caching or consolidation, the extra service hop can cancel the transport-efficiency advantage. |
| High concurrency | Potentially higher and more stable throughput until the backend saturates | The service can pool connections, batch work and enforce centralized backpressure; once the same database is saturated, both approaches share that bottleneck. |

pRPC's largest gains come from moving a complete data operation behind one typed call—not from wrapping each SQL statement in an RPC. Concrete claims should still be validated on the deployment hardware with the same schema, payloads and concurrency, reporting throughput plus p50, p95 and p99 latency.

## Production Checklist

- Install and enable `ext-grpc`; enable `ext-protobuf` for the lowest protobuf overhead.
- Use TLS or mTLS credentials for traffic that crosses an untrusted network.
- Keep deadlines short and appropriate for the backend so shutdown and failure recovery remain bounded.
- Set request, response, metadata and outstanding-call limits from measured production payloads and capacity.
- Call `close()` when the plugin disables.
- Run load and soak tests against the real PocketMine build and gRPC service before deployment.

## License

Apache-2.0
