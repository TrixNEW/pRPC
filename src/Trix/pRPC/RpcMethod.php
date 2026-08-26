<?php

declare(strict_types=1);

namespace Trix\pRPC;

use Google\Protobuf\Internal\Message;
use InvalidArgumentException;

/**
 * Describes one unary protobuf RPC.
 *
 * @template TRequest of Message
 * @template TResponse of Message
 */
final readonly class RpcMethod{
    /**
     * @param class-string<TRequest> $requestClass
     * @param class-string<TResponse> $responseClass
     */
    public function __construct(
        public string $path,
        public string $requestClass,
        public string $responseClass
    ){
        if(preg_match('~^/[A-Za-z_][A-Za-z0-9_.]*/[A-Za-z_][A-Za-z0-9_]*$~', $path) !== 1){
            throw new InvalidArgumentException("Invalid gRPC method path '$path'. Expected /package.Service/Method.");
        }
        if(!is_a($requestClass, Message::class, true)){
            throw new InvalidArgumentException("{$requestClass} must extend " . Message::class . '.');
        }
        if(!is_a($responseClass, Message::class, true)){
            throw new InvalidArgumentException("{$responseClass} must extend " . Message::class . '.');
        }
    }

    public function name() : string{
        $pos = strrpos($this->path, '/');
        return $pos === false ? $this->path : substr($this->path, $pos + 1);
    }
}
