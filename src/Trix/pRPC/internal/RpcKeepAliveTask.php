<?php

declare(strict_types=1);

namespace Trix\pRPC\internal;

use pocketmine\scheduler\AsyncTask;

/**
 * Keeps a PocketMine async worker from being stopped as idle, a replacement thread would swap ext-grpc's completion
 * queue under calls in flight.
 *
 * @internal
 */
final class RpcKeepAliveTask extends AsyncTask{
    public function onRun() : void{}
}
