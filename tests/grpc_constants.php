<?php

declare(strict_types=1);

// stand-ins so the tests run without ext-grpc
namespace Grpc;

if(!\extension_loaded('grpc')){
    foreach([
        'STATUS_OK' => 0, 'STATUS_CANCELLED' => 1, 'STATUS_UNKNOWN' => 2, 'STATUS_INVALID_ARGUMENT' => 3,
        'STATUS_DEADLINE_EXCEEDED' => 4, 'STATUS_NOT_FOUND' => 5, 'STATUS_ALREADY_EXISTS' => 6,
        'STATUS_PERMISSION_DENIED' => 7, 'STATUS_RESOURCE_EXHAUSTED' => 8, 'STATUS_FAILED_PRECONDITION' => 9,
        'STATUS_ABORTED' => 10, 'STATUS_OUT_OF_RANGE' => 11, 'STATUS_UNIMPLEMENTED' => 12, 'STATUS_INTERNAL' => 13,
        'STATUS_UNAVAILABLE' => 14, 'STATUS_DATA_LOSS' => 15, 'STATUS_UNAUTHENTICATED' => 16,
    ] as $name => $value){
        if(!\defined("Grpc\\$name")){
            \define("Grpc\\$name", $value);
        }
    }
}
