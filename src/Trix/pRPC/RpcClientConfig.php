<?php

declare(strict_types=1);

namespace Trix\pRPC;

use Grpc\BaseStub;
use InvalidArgumentException;

final readonly class RpcClientConfig{
    public RpcCredentials $credentials;

    /** @var array<string, mixed> */
    public array $channelOptions;

    /**
     * @param class-string<BaseStub> $stubClass
     * @param array<string, mixed> $channelOptions Only scalar/null/nested-array values are allowed.
     */
    public function __construct(
        public string $endpoint,
        public string $stubClass,
        ?RpcCredentials $credentials = null,
        array $channelOptions = [],
        public int $workers = 2,
        public int $defaultTimeoutMs = 3_000,
        public int $maxTimeoutMs = 30_000,
        public int $maxPending = 8_192,
        public int $batchSize = 32
    ){
        if($endpoint === ''){
            throw new InvalidArgumentException('endpoint cannot be empty.');
        }
        if(!is_a($stubClass, BaseStub::class, true)){
            throw new InvalidArgumentException("{$stubClass} must extend " . BaseStub::class . '.');
        }
        if($workers < 1 || $workers > 16){
            throw new InvalidArgumentException('workers must be between 1 and 16.');
        }
        if($defaultTimeoutMs <= 0){
            throw new InvalidArgumentException('defaultTimeoutMs must be greater than 0.');
        }
        if($maxTimeoutMs < $defaultTimeoutMs){
            throw new InvalidArgumentException('maxTimeoutMs must be >= defaultTimeoutMs.');
        }
        if($maxPending < 1){
            throw new InvalidArgumentException('maxPending must be greater than 0.');
        }
        if($batchSize < 1 || $batchSize > 1_024){
            throw new InvalidArgumentException('batchSize must be between 1 and 1024.');
        }
        if(array_key_exists('credentials', $channelOptions)){
            throw new InvalidArgumentException("Do not put 'credentials' in channelOptions; use RpcCredentials instead.");
        }

        self::validateThreadSafeValue($channelOptions, 'channelOptions');

        $this->credentials = $credentials ?? RpcCredentials::insecure();
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
}
