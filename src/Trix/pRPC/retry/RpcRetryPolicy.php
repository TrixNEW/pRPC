<?php

declare(strict_types=1);

namespace Trix\pRPC\retry;

use InvalidArgumentException;

// only turn on retryAmbiguous when the server dedupes requests, or a retry can apply twice
final readonly class RpcRetryPolicy{
    public function __construct(
        public int $maxAttempts = 5,
        public int $baseDelayMs = 100,
        public int $maxDelayMs = 5_000,
        public bool $retryAmbiguous = false,
        public float $jitter = 0.2,
        public int $maxUncertainAttempts = 3
    ){
        if($maxAttempts < 1 || $maxUncertainAttempts < 1){
            throw new InvalidArgumentException('maxAttempts and maxUncertainAttempts must be at least 1.');
        }
        if($baseDelayMs < 1 || $maxDelayMs < $baseDelayMs){
            throw new InvalidArgumentException('Invalid retry delay range.');
        }
        if($jitter < 0.0 || $jitter > 1.0){
            throw new InvalidArgumentException('jitter must be between 0 and 1.');
        }
    }

    public function delayMs(int $retry, ?float $random = null) : int{
        $exp = min($this->maxDelayMs, $this->baseDelayMs * (2 ** min(30, $retry - 1)));
        $random ??= mt_rand() / mt_getrandmax();
        $spread = (int) round($exp * $this->jitter * ($random * 2 - 1));
        return max(1, min($this->maxDelayMs, $exp + $spread));
    }
}
