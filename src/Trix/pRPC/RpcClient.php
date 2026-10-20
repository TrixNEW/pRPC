<?php

declare(strict_types=1);

namespace Trix\pRPC;

use Closure;
use Google\Protobuf\Internal\Message;
use InvalidArgumentException;
use pocketmine\plugin\PluginBase;
use RuntimeException;
use Throwable;
use Trix\pRPC\internal\RpcWorkerPool;
use Trix\pRPC\opts\RpcCallOptions;
use Trix\pRPC\opts\RpcClientConfig;
use Trix\pRPC\retry\RpcRetrier;
use Trix\pRPC\retry\RpcRetryPolicy;
use Trix\pRPC\retry\RpcStatus;
use Trix\pRPC\stats\RpcClientStats;
use Trix\pRPC\stats\RpcLatency;
use Trix\pRPC\thread\GrpcWorkerThread;
use Trix\pRPC\utils\exception\RpcClosedException;
use Trix\pRPC\utils\exception\RpcException;
use Trix\pRPC\utils\exception\RpcMessageTooLargeException;
use Trix\pRPC\utils\exception\RpcOverloadedException;
use Trix\pRPC\utils\exception\RpcTimeoutException;
use Trix\pRPC\utils\exception\RpcWorkerException;
use Trix\pRPC\utils\promise\RpcPromise;
use Trix\pRPC\utils\promise\RpcPromiseResolver;

final class RpcClient{
    private const int PROBE_INTERVAL_NS = 50_000_000;

    private readonly RpcWorkerPool $pool;
    private readonly bool $ownsPool;
    private readonly int $endpointId;

    /** @var array<string, int> */
    private array $methodIdsByPath = [];

    /** @var array<string, RpcMethod> */
    private array $methodsByPath = [];

    /**
     * @var array<int, array{
     *     resolver: RpcPromiseResolver<Message>,
     *     responseClass: class-string<Message>,
     *     deadlineNs: int,
     *     timeoutMs: int,
     *     startNs: int
     * }>
     */
    private array $pending = [];

    /** @var array<int, int> request id => queued bytes, until a worker hands the request back */
    private array $transport = [];
    private int $transportBytes = 0;
    private int $nextDeadlineNs = PHP_INT_MAX;
    private int $nextId = 1;
    private int $lastProbeNs = 0;
    private bool $closed = false;

    private int $completed = 0;
    private int $failed = 0;
    private int $timeouts = 0;
    private int $overloaded = 0;
    private int $transportErrors = 0;
    private readonly RpcLatency $latency;
    private readonly RpcLatency $queueWait;

    /** @var Closure(Throwable): void */
    private Closure $callbackErrorHandler;

    private readonly RpcRetrier $retrier;

    /**
     * Pass your plugin to get a client with its own workers. RpcClientManager passes its shared pool instead.
     *
     * @throws Throwable
     */
    public function __construct(PluginBase|RpcWorkerPool $owner, private readonly RpcClientConfig $config, public readonly string $name = 'default'){
        $this->callbackErrorHandler = static function(Throwable $e) : void{
            \GlobalLogger::get()->logException($e);
        };
        $this->retrier = new RpcRetrier($this->callbackErrorHandler, static fn() : int => hrtime(true));
        $this->latency = new RpcLatency();
        $this->queueWait = new RpcLatency();

        $methodPaths = [];
        foreach($config->methods as $id => $method){
            $this->methodIdsByPath[$method->path] = $id;
            $this->methodsByPath[$method->path] = $method;
            $methodPaths[$id] = $method->path;
        }

        $this->ownsPool = $owner instanceof PluginBase;
        $this->pool = $owner instanceof PluginBase
            ? new RpcWorkerPool($owner, min($config->workers, RpcWorkerPool::MAX_WORKERS), $config->maxOutstanding, $config->maxQueuedBytes)
            : $owner;

        try{
            $this->endpointId = $this->pool->attach($this, serialize([
                'endpoint' => $config->endpoint,
                'credentials' => $config->credentials->export(),
                'channelOptions' => $config->channelOptions,
                'methodPaths' => $methodPaths,
                'maxResponseBytes' => $config->maxResponseBytes,
                'maxMetadataBytes' => $config->maxMetadataBytes,
                'reconnectBackoffMinMs' => $config->reconnectBackoffMinMs,
                'reconnectBackoffMaxMs' => $config->reconnectBackoffMaxMs,
            ]), $config->workers, $config->batchSize);
        }catch(Throwable $e){
            if($this->ownsPool){
                $this->pool->close(0);
            }
            throw $e;
        }
    }

    /**
     * Executes a registered unary RPC without blocking the PocketMine main thread.
     * The request is encoded once on the main thread and the response is decoded once on the main thread.
     *
     * @template TRequest of Message
     * @template TResponse of Message
     * @param RpcMethod<TRequest, TResponse> $method
     * @param TRequest $request
     * @return RpcPromise<TResponse>
     */
    public function call(RpcMethod $method, Message $request, ?RpcCallOptions $options = null) : RpcPromise{
        if($this->closed){
            return $this->rejected(new RpcClosedException());
        }

        $registered = $this->methodsByPath[$method->path] ?? null;
        if($registered === null || $registered->requestClass !== $method->requestClass || $registered->responseClass !== $method->responseClass){
            throw new InvalidArgumentException("RPC method '$method->path' is not registered in this client.");
        }
        if(!$request instanceof $method->requestClass){
            throw new InvalidArgumentException("RPC '$method->path' expects $method->requestClass, got " . $request::class . '.');
        }
        if(count($this->transport) >= $this->config->maxOutstanding){
            ++$this->overloaded;
            return $this->rejected(new RpcOverloadedException($this->config->maxOutstanding));
        }

        $options ??= RpcCallOptions::none();
        $timeoutMs = $options->timeoutMs ?? $this->config->defaultTimeoutMs;
        if($timeoutMs > $this->config->maxTimeoutMs){
            throw new InvalidArgumentException("timeoutMs cannot exceed {$this->config->maxTimeoutMs} ms.");
        }
        $metadataBytes = $options->metadataBytes();
        if($metadataBytes > $this->config->maxMetadataBytes){
            return $this->rejected(new RpcMessageTooLargeException('metadata', $metadataBytes, $this->config->maxMetadataBytes));
        }
        if(!$this->pool->canServe()){
            return $this->rejected(new RpcWorkerException('No healthy gRPC workers are available.', sent: false));
        }

        try{
            $requestData = $request->serializeToString();
        }catch(Throwable $e){
            return $this->rejected(new RpcException('Request serialization failed: ' . $e->getMessage(), RpcStatus::INTERNAL));
        }
        $requestBytes = strlen($requestData);
        if($requestBytes > $this->config->maxRequestBytes){
            return $this->rejected(new RpcMessageTooLargeException('request', $requestBytes, $this->config->maxRequestBytes));
        }
        $jobBytes = $requestBytes + strlen($options->packedMetadata) + GrpcWorkerThread::JOB_HEADER_BYTES;
        // one request bigger than the byte budget still goes through on its own, or it could never be sent
        if($this->transport !== [] && $this->transportBytes + $jobBytes > $this->config->maxQueuedBytes){
            ++$this->overloaded;
            return $this->rejected(new RpcOverloadedException($this->config->maxQueuedBytes, 'byte'));
        }
        $poolFull = $this->pool->isFull($jobBytes);
        if($poolFull !== null){
            ++$this->overloaded;
            return $this->rejected(new RpcOverloadedException($poolFull === 'byte' ? $this->pool->maxQueuedBytes : $this->pool->maxOutstanding, $poolFull));
        }

        $id = $this->allocateId();
        $now = hrtime(true);
        $deadlineNs = $now + $timeoutMs * 1_000_000;
        $job = pack('JJJNN', $id, $deadlineNs, $now, $this->methodIdsByPath[$method->path], strlen($options->packedMetadata)) . $options->packedMetadata . $requestData;
        if(!$this->pool->submit($this->endpointId, $job, $jobBytes)){
            return $this->rejected(new RpcWorkerException('Failed to queue RPC: the worker pool is stopping.', sent: false));
        }

        /** @var RpcPromiseResolver<Message> $resolver */
        $resolver = new RpcPromiseResolver($this->callbackErrorHandler);
        $this->pending[$id] = [
            'resolver' => $resolver,
            'responseClass' => $method->responseClass,
            'deadlineNs' => $deadlineNs,
            'timeoutMs' => $timeoutMs,
            'startNs' => $now,
        ];
        $this->transport[$id] = $jobBytes;
        $this->transportBytes += $jobBytes;
        if($deadlineNs < $this->nextDeadlineNs){
            $this->nextDeadlineNs = $deadlineNs;
        }

        return $resolver->promise();
    }

    /**
     * Retries resend this exact request, so put the idempotency key in it first.
     *
     * @template TRequest of Message
     * @template TResponse of Message
     * @param RpcMethod<TRequest, TResponse> $method
     * @param TRequest $request
     * @param ?Closure(Throwable, int): void $onRetry
     * @return RpcPromise<TResponse>
     */
    public function callRetrying(RpcMethod $method, Message $request, ?RpcRetryPolicy $policy = null, ?RpcCallOptions $options = null, ?Closure $onRetry = null) : RpcPromise{
        return $this->retrier->run(fn() => $this->call($method, $request, $options), $policy ?? new RpcRetryPolicy(), $onRetry);
    }

    /**
     * True once the channel reports a live connection to the server. Starts connecting and returns false when it is
     * still idle, so poll it rather than checking once.
     */
    public function isReady() : bool{
        if($this->closed || !$this->pool->canServe()){
            return false;
        }
        if($this->pool->connectivity($this->endpointId) === GrpcWorkerThread::CONNECTIVITY_READY){
            return true;
        }
        $now = hrtime(true);
        if($now - $this->lastProbeNs >= self::PROBE_INTERVAL_NS){
            $this->lastProbeNs = $now;
            $this->pool->probe($this->endpointId);
        }
        return false;
    }

    /** Blocks the thread, so only use this at startup or shutdown. */
    public function drain(int $timeoutMs) : bool{
        $deadline = hrtime(true) + max(0, $timeoutMs) * 1_000_000;
        while(!$this->closed && ($this->pending !== [] || $this->retrier->waiting() > 0)){
            $this->pool->pump();
            if($this->pending === [] && $this->retrier->waiting() === 0){
                break;
            }
            if(hrtime(true) >= $deadline){
                return false;
            }
            usleep(200);
        }
        return $this->pending === [] && $this->retrier->waiting() === 0;
    }

    public function pendingCount() : int{
        return count($this->pending);
    }

    /** Counts jobs we've already timed out locally but the worker hasn't released yet. */
    public function outstandingCount() : int{
        return count($this->transport);
    }

    public function isClosed() : bool{
        return $this->closed;
    }

    public function stats() : RpcClientStats{
        [$loads, $states] = $this->pool->workerStats();
        $lane = $this->pool->laneStats($this->endpointId);
        return new RpcClientStats(
            count($this->pending),
            count($this->transport),
            $loads,
            $states,
            $this->retrier->waiting(),
            $this->name,
            GrpcWorkerThread::connectivityName($lane['connectivity']),
            $lane['queued'],
            $lane['busy'],
            $this->transportBytes,
            $this->pool->liveWorkers(),
            $this->pool->lostWorkers(),
            $this->completed,
            $this->failed,
            $this->timeouts,
            $this->overloaded,
            $this->transportErrors,
            $this->retrier->retries(),
            $lane['reconnects'],
            clone $this->latency,
            clone $this->queueWait
        );
    }

    /**
     * Stops taking new work, waits up to the grace period for calls in flight, then rejects whatever is left.
     *
     * PHP gRPC's UnaryCall::wait() blocks inside the worker and can't be interrupted from another thread, so a
     * worker still inside a call is left to finish it in the background rather than blocking the server until its
     * deadline. Closing a client from an RpcClientManager leaves the shared workers running.
     */
    public function close(int $gracePeriodMs = 500) : void{
        if($this->closed){
            return;
        }
        if($gracePeriodMs < 0){
            throw new InvalidArgumentException('gracePeriodMs cannot be negative.');
        }
        if($this->ownsPool){
            $this->pool->close($gracePeriodMs);
            return;
        }

        $this->markClosed();
        $deadline = hrtime(true) + $gracePeriodMs * 1_000_000;
        while($this->transport !== [] && hrtime(true) < $deadline && !$this->pool->isClosed()){
            $this->pool->pump(false);
            usleep(250);
        }
        $this->onDropped($this->pool->detach($this->endpointId));
        $this->abandonTransport();
    }

    /** @internal */
    public function markClosed() : void{
        $this->closed = true;
        $this->retrier->stop();
    }

    /** @internal */
    public function onTick(int $now) : void{
        if($now >= $this->nextDeadlineNs){
            $this->expireDeadlines($now);
        }
        $this->retrier->tick();
    }

    /** @internal */
    public function onResult(int $id, int $code, int $queueWaitUs, string $body) : void{
        if(!$this->releaseTransport($id)){
            return;
        }
        $this->queueWait->add($queueWaitUs);
        $pending = $this->pending[$id] ?? null;
        if($pending === null){
            return;
        }
        unset($this->pending[$id]);
        $this->latency->add(intdiv(hrtime(true) - $pending['startNs'], 1_000));
        $resolver = $pending['resolver'];

        if($code !== RpcStatus::OK){
            ++$this->failed;
            if($code === RpcStatus::DEADLINE_EXCEEDED){
                ++$this->timeouts;
            }elseif($code === RpcStatus::UNAVAILABLE){
                ++$this->transportErrors;
            }
            $error = unserialize($body, ['allowed_classes' => false]);
            $resolver->reject(new RpcException(
                is_string($error[0] ?? null) ? $error[0] : 'Unknown gRPC error.',
                $code,
                is_array($error[1] ?? null) ? $error[1] : []
            ));
            return;
        }

        $responseBytes = strlen($body);
        if($responseBytes > $this->config->maxResponseBytes){
            ++$this->failed;
            $resolver->reject(new RpcMessageTooLargeException('response', $responseBytes, $this->config->maxResponseBytes));
            return;
        }
        try{
            $responseClass = $pending['responseClass'];
            $response = new $responseClass();
            $response->mergeFromString($body);
        }catch(Throwable $e){
            ++$this->failed;
            $resolver->reject(new RpcException('Response decode failed: ' . $e->getMessage(), RpcStatus::INTERNAL));
            return;
        }
        ++$this->completed;
        $resolver->resolve($response);
    }

    /**
     * A worker died or hung with these requests, so they will never come back.
     *
     * @internal
     * @param list<int> $ids
     */
    public function onTransportLost(array $ids, string $reason) : void{
        $error = new RpcWorkerException($reason);
        foreach($ids as $id){
            if(!$this->releaseTransport($id)){
                continue;
            }
            $pending = $this->pending[$id] ?? null;
            if($pending !== null){
                unset($this->pending[$id]);
                ++$this->failed;
                ++$this->transportErrors;
                $pending['resolver']->reject($error);
            }
        }
    }

    /**
     * @internal
     * @param list<string> $jobs
     */
    public function onDropped(array $jobs) : void{
        foreach($jobs as $job){
            if(strlen($job) >= GrpcWorkerThread::JOB_HEADER_BYTES){
                $this->releaseTransport(unpack('J', $job)[1]);
            }
        }
    }

    /**
     * Rejects everything still pending, and stops counting requests a worker may still be finishing.
     *
     * @internal
     */
    public function abandonTransport() : void{
        foreach($this->transport as $bytes){
            $this->pool->release($bytes);
        }
        $this->transport = [];
        $this->transportBytes = 0;

        $pending = $this->pending;
        $this->pending = [];
        $error = new RpcClosedException('RPC client closed before the request completed.');
        foreach($pending as $entry){
            $entry['resolver']->reject($error);
        }
    }

    private function releaseTransport(int $id) : bool{
        $bytes = $this->transport[$id] ?? null;
        if($bytes === null){
            return false;
        }
        unset($this->transport[$id]);
        $this->transportBytes -= $bytes;
        $this->pool->release($bytes);
        return true;
    }

    /** Most ticks only compare against the earliest deadline, the scan runs once something is actually due. */
    private function expireDeadlines(int $now) : void{
        $next = PHP_INT_MAX;
        // calls made from the callbacks below lower this again
        $this->nextDeadlineNs = PHP_INT_MAX;
        foreach($this->pending as $id => $pending){
            if($pending['deadlineNs'] > $now){
                if($pending['deadlineNs'] < $next){
                    $next = $pending['deadlineNs'];
                }
                continue;
            }
            // a callback may have closed the client and settled it already
            if(!isset($this->pending[$id])){
                continue;
            }
            unset($this->pending[$id]);
            ++$this->failed;
            ++$this->timeouts;
            $pending['resolver']->reject(new RpcTimeoutException($pending['timeoutMs']));
        }
        $this->nextDeadlineNs = min($this->nextDeadlineNs, $next);
    }

    private function allocateId() : int{
        if($this->nextId === PHP_INT_MAX){
            if($this->transport !== []){
                throw new RuntimeException('RPC request ID space exhausted.');
            }
            $this->nextId = 1;
        }
        return $this->nextId++;
    }

    /** @template TValue @return RpcPromise<TValue> */
    private function rejected(Throwable $error) : RpcPromise{
        /** @var RpcPromiseResolver<TValue> $resolver */
        $resolver = new RpcPromiseResolver($this->callbackErrorHandler);
        $resolver->reject($error);
        return $resolver->promise();
    }
}
