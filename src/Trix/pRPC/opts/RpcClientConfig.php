<?php

declare(strict_types=1);

namespace Trix\pRPC\opts;

use InvalidArgumentException;
use Trix\pRPC\RpcMethod;

final readonly class RpcClientConfig{
    public RpcCredentials $credentials;

    /** @var list<RpcMethod> */
    public array $methods;

    /** @var array<string, mixed> */
    public array $channelOptions;

    /**
     * @param list<RpcMethod> $methods
     * @param array<string, mixed> $channelOptions Scalar/null/nested-array values only.
     */
    public function __construct(
        public string $endpoint,
        array $methods,
        ?RpcCredentials $credentials = null,
        array $channelOptions = [],
        public int $workers = 2,
        public int $batchSize = 4,
        public int $defaultTimeoutMs = 1_500,
        public int $maxTimeoutMs = 5_000,
        public int $maxOutstanding = 4_096,
        public int $maxRequestBytes = 4_194_304,
        public int $maxResponseBytes = 4_194_304,
        public int $maxMetadataBytes = 32_768,
        public int $reconnectBackoffMinMs = 100,
        public int $reconnectBackoffMaxMs = 5_000
    ){
        if($endpoint === ''){
            throw new InvalidArgumentException('endpoint cannot be empty.');
        }
        if($methods === []){
            throw new InvalidArgumentException('At least one RpcMethod must be registered.');
        }
        if($workers < 1 || $workers > 16){
            throw new InvalidArgumentException('workers must be between 1 and 16.');
        }
        if($batchSize < 1 || $batchSize > 64){
            throw new InvalidArgumentException('batchSize must be between 1 and 64.');
        }
        if($defaultTimeoutMs <= 0){
            throw new InvalidArgumentException('defaultTimeoutMs must be greater than 0.');
        }
        if($maxTimeoutMs > intdiv(PHP_INT_MAX, 1_000_000)){
            throw new InvalidArgumentException('maxTimeoutMs is too large for the monotonic deadline clock.');
        }
        if($maxTimeoutMs < $defaultTimeoutMs){
            throw new InvalidArgumentException('maxTimeoutMs must be >= defaultTimeoutMs.');
        }
        if($maxOutstanding < 1){
            throw new InvalidArgumentException('maxOutstanding must be greater than 0.');
        }
        if($maxRequestBytes < 1 || $maxResponseBytes < 1 || $maxMetadataBytes < 1){
            throw new InvalidArgumentException('Message and metadata byte limits must be greater than 0.');
        }
        if($reconnectBackoffMinMs < 1 || $reconnectBackoffMaxMs < $reconnectBackoffMinMs){
            throw new InvalidArgumentException('Invalid reconnect backoff range.');
        }
        if(array_key_exists('credentials', $channelOptions)){
            throw new InvalidArgumentException("Do not put 'credentials' in channelOptions; use RpcCredentials instead.");
        }

        $normalizedMethods = [];
        $seenPaths = [];
        foreach($methods as $method){
            if(!$method instanceof RpcMethod){
                throw new InvalidArgumentException('methods must contain only RpcMethod instances.');
            }
            if(isset($seenPaths[$method->path])){
                throw new InvalidArgumentException("Duplicate RPC method path '$method->path'.");
            }
            $seenPaths[$method->path] = true;
            $normalizedMethods[] = $method;
        }

        self::validateThreadSafeValue($channelOptions, 'channelOptions');
        self::normalizeMessageLimit($channelOptions, 'grpc.max_send_message_length', $maxRequestBytes);
        self::normalizeMessageLimit($channelOptions, 'grpc.max_receive_message_length', $maxResponseBytes);

        $this->credentials = $credentials ?? RpcCredentials::insecure();
        $this->methods = $normalizedMethods;
        $this->channelOptions = $channelOptions;
    }

    private static function validateThreadSafeValue(mixed $value, string $path) : void{
        if($value === null || is_scalar($value)){
            return;
        }
        if(!is_array($value)){
            throw new InvalidArgumentException("$path contains an object/resource/callback that cannot cross a thread boundary.");
        }
        foreach($value as $key => $child){
            self::validateThreadSafeValue($child, $path . '[' . $key . ']');
        }
    }

    /** @param array<string, mixed> $channelOptions */
    private static function normalizeMessageLimit(array &$channelOptions, string $key, int $configuredLimit) : void{
        $value = $channelOptions[$key] ?? $configuredLimit;
        if(!is_int($value) || $value < 1){
            throw new InvalidArgumentException("channelOptions['$key'] must be a positive integer.");
        }
        $channelOptions[$key] = min($value, $configuredLimit);
    }
}
