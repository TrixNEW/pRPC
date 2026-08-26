<?php

declare(strict_types=1);

namespace Trix\pRPC\opts;

use InvalidArgumentException;

final readonly class RpcCallOptions{
    public ?int $timeoutMs;

    /** @var array<string, list<string>> */
    public array $metadata;

    /**
     * @param array<string, string|list<string>> $metadata
     */
    public function __construct(?int $timeoutMs = null, array $metadata = []){
        if($timeoutMs !== null && $timeoutMs <= 0){
            throw new InvalidArgumentException('timeoutMs must be greater than 0.');
        }

        $normalized = [];
        foreach($metadata as $key => $values){
            if(!is_string($key) || $key === '' || preg_match('/^[.A-Za-z\d_-]+$/', $key) !== 1){
                throw new InvalidArgumentException("Invalid gRPC metadata key: $key");
            }

            $values = is_string($values) ? [$values] : $values;
            if(!is_array($values) || $values === []){
                throw new InvalidArgumentException("Metadata '$key' must contain at least one string value.");
            }

            $normalizedValues = [];
            foreach($values as $value){
                if(!is_string($value)){
                    throw new InvalidArgumentException("Metadata '$key' values must be strings.");
                }
                $normalizedValues[] = $value;
            }
            $normalized[strtolower($key)] = $normalizedValues;
        }

        $this->timeoutMs = $timeoutMs;
        $this->metadata = $normalized;
    }

    /** @internal */
    public function metadataBytes() : int{
        $bytes = 0;
        foreach($this->metadata as $key => $values){
            $bytes += strlen($key);
            foreach($values as $value){
                $bytes += strlen($value);
            }
        }
        return $bytes;
    }
}
