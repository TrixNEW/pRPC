<?php

declare(strict_types=1);

namespace Trix\Example;

use Example\Data\GetPlayerRequest;
use Example\Data\GetPlayerResponse;
use Example\Data\SavePlayerRequest;
use Example\Data\SavePlayerResponse;
use pocketmine\plugin\PluginBase;
use Throwable;
use Trix\pRPC\RpcCallOptions;
use Trix\pRPC\RpcClient;
use Trix\pRPC\RpcClientConfig;
use Trix\pRPC\RpcCredentials;
use Trix\pRPC\RpcMethod;

final class Main extends PluginBase{

    private RpcClient $rpc;

    private RpcMethod $getPlayer;
    private RpcMethod $savePlayer;

    protected function onEnable() : void{
        $this->getPlayer = new RpcMethod(
            "/data.DataService/GetPlayer",
            GetPlayerRequest::class,
            GetPlayerResponse::class
        );

        $this->savePlayer = new RpcMethod(
            "/data.DataService/SavePlayer",
            SavePlayerRequest::class,
            SavePlayerResponse::class
        );

        $this->rpc = new RpcClient(
            $this,
            new RpcClientConfig(
                endpoint: "127.0.0.1:50051",
                methods: [$this->getPlayer, $this->savePlayer],
                credentials: RpcCredentials::insecure(),
                workers: 4,
                batchSize: 4,
                defaultTimeoutMs: 1500,
                maxTimeoutMs: 5000,
                maxOutstanding: 4096
            )
        );
    }

    public function loadPlayer(string $xuid) : void{
        $request = new GetPlayerRequest();
        $request->setXuid($xuid);

        $this->rpc->call($this->getPlayer, $request)->then(function(GetPlayerResponse $response) : void{
            $name = $response->getName();
            $coins = $response->getCoins();

            $this->getLogger()->info("Loaded {$name} with {$coins} coins");
        })->onError(function(Throwable $error) : void{
            $this->getLogger()->error("Failed to load player: " . $error->getMessage());
        });
    }

    public function savePlayer(string $xuid, string $name, int $coins) : void{
        $request = new SavePlayerRequest();

        $request->setXuid($xuid);
        $request->setName($name);
        $request->setCoins($coins);

        $this->rpc->call(
            $this->savePlayer,
            $request,
            new RpcCallOptions(
                timeoutMs: 1000,
                metadata: [
                    "server-id" => "factions-1"
                ]
            )
        )->then(function(SavePlayerResponse $response) : void{
            if(!$response->getSuccess()){
                $this->getLogger()->warning("Player save was rejected");
            }
        })->onError(function(Throwable $error) : void{
            $this->getLogger()->error("Failed to save player: " . $error->getMessage());
        });
    }

    protected function onDisable() : void{
        $this->rpc->close();
    }
}