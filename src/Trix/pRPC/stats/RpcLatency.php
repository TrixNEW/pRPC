<?php

declare(strict_types=1);

namespace Trix\pRPC\stats;

/**
 * Fixed size latency histogram with four buckets per power of two, so percentiles are within ~25%.
 */
final class RpcLatency{
    /** @var array<int, int> bucket => samples */
    private array $buckets = [];
    private int $count = 0;
    private int $sumUs = 0;
    private int $maxUs = 0;

    /** @internal */
    public function add(int $us) : void{
        if($us < 0){
            $us = 0;
        }
        ++$this->count;
        $this->sumUs += $us;
        if($us > $this->maxUs){
            $this->maxUs = $us;
        }
        if($us < 4){
            $bucket = $us;
        }else{
            $exp = (int) log($us, 2);
            if((1 << $exp) > $us){
                --$exp;
            }elseif((2 << $exp) <= $us){
                ++$exp;
            }
            $bucket = ($exp << 2) | (($us >> ($exp - 2)) & 3);
        }
        $this->buckets[$bucket] = ($this->buckets[$bucket] ?? 0) + 1;
    }

    public function count() : int{
        return $this->count;
    }

    public function averageMs() : float{
        return $this->count === 0 ? 0.0 : $this->sumUs / $this->count / 1000;
    }

    public function maxMs() : float{
        return $this->maxUs / 1000;
    }

    /** Upper bound of the bucket holding the given percentile, e.g. percentileMs(0.99). */
    public function percentileMs(float $q) : float{
        if($this->count === 0){
            return 0.0;
        }
        ksort($this->buckets);
        $rank = max(1, (int) ceil($q * $this->count));
        $seen = 0;
        foreach($this->buckets as $bucket => $n){
            $seen += $n;
            if($seen >= $rank){
                $upperUs = $bucket < 4 ? $bucket + 1 : (5 + ($bucket & 3)) << (($bucket >> 2) - 2);
                return min($upperUs, $this->maxUs) / 1000;
            }
        }
        return $this->maxUs / 1000;
    }
}
