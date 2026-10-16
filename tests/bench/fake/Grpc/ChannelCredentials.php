<?php

declare(strict_types=1);

namespace Grpc;

require_once __DIR__ . '/consts.php';

class ChannelCredentials{
    public static function createInsecure() : ?self{
        return null;
    }

    public static function createSsl(?string $roots = null, ?string $key = null, ?string $chain = null) : self{
        return new self();
    }
}
