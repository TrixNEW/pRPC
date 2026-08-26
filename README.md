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
- Result notifications are coalesced to reduce thread wakeup overhead.
- Worker selection avoids per-request cross-thread load polling.
- Native + local deadlines prevent indefinitely pending RPCs.
- Request, response and metadata sizes are bounded.
- Promise callback exceptions are isolated from unrelated RPC completions.
- Worker reconnects use exponential backoff.

For a local/LAN database service, the defaults are intentionally latency-oriented. Start around **2–4 workers** and **batch size 4**, then benchmark against the real backend before increasing concurrency.

## License

Apache-2.0
