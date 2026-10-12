<?php

declare(strict_types=1);

namespace Trix\pRPC\retry;

use Throwable;
use Trix\pRPC\utils\exception\RpcClosedException;
use Trix\pRPC\utils\exception\RpcException;
use Trix\pRPC\utils\exception\RpcMessageTooLargeException;
use Trix\pRPC\utils\exception\RpcOverloadedException;
use Trix\pRPC\utils\exception\RpcTimeoutException;
use Trix\pRPC\utils\exception\RpcWorkerException;

final class RpcStatus{
    public const int OK = 0;
    public const int CANCELLED = 1;
    public const int UNKNOWN = 2;
    public const int INVALID_ARGUMENT = 3;
    public const int DEADLINE_EXCEEDED = 4;
    public const int NOT_FOUND = 5;
    public const int ALREADY_EXISTS = 6;
    public const int PERMISSION_DENIED = 7;
    public const int RESOURCE_EXHAUSTED = 8;
    public const int FAILED_PRECONDITION = 9;
    public const int ABORTED = 10;
    public const int OUT_OF_RANGE = 11;
    public const int UNIMPLEMENTED = 12;
    public const int INTERNAL = 13;
    public const int UNAVAILABLE = 14;
    public const int DATA_LOSS = 15;
    public const int UNAUTHENTICATED = 16;

    private function __construct(){}

    public static function code(Throwable $e) : int{
        return $e instanceof RpcException ? $e->getStatusCode() : self::UNKNOWN;
    }

    public static function notSent(Throwable $e) : bool{
        return $e instanceof RpcOverloadedException || $e instanceof RpcMessageTooLargeException;
    }

    public static function isAmbiguous(Throwable $e) : bool{
        if(self::notSent($e)){
            return false;
        }
        if($e instanceof RpcTimeoutException || $e instanceof RpcWorkerException || $e instanceof RpcClosedException){
            return true;
        }
        return match(self::code($e)){
            self::DEADLINE_EXCEEDED, self::UNAVAILABLE, self::UNKNOWN, self::INTERNAL, self::CANCELLED => true,
            default => false,
        };
    }

    public static function isRetryable(Throwable $e) : bool{
        if($e instanceof RpcMessageTooLargeException){
            return false;
        }
        if($e instanceof RpcOverloadedException || $e instanceof RpcTimeoutException || $e instanceof RpcWorkerException){
            return true;
        }
        if($e instanceof RpcClosedException){
            return false;
        }
        return match(self::code($e)){
            self::DEADLINE_EXCEEDED, self::UNAVAILABLE, self::RESOURCE_EXHAUSTED, self::ABORTED => true,
            default => false,
        };
    }
}
