<?php

declare(strict_types=1);

namespace Trix\pRPC;

use InvalidArgumentException;
use RuntimeException;

final readonly class RpcCredentials{
    private const string MODE_INSECURE = 'insecure';
    private const string MODE_TLS = 'tls';

    private function __construct(
        private string $mode,
        private ?string $rootCertificates = null,
        private ?string $privateKey = null,
        private ?string $certificateChain = null
    ){
        if(($privateKey === null) !== ($certificateChain === null)){
            throw new InvalidArgumentException('A TLS private key and certificate chain must be provided together.');
        }
    }

    public static function insecure() : self{
        return new self(self::MODE_INSECURE);
    }

    public static function tls(
        ?string $rootCertificates = null,
        ?string $privateKey = null,
        ?string $certificateChain = null
    ) : self{
        return new self(self::MODE_TLS, $rootCertificates, $privateKey, $certificateChain);
    }

    public static function tlsFromFiles(
        ?string $rootCertificatesFile = null,
        ?string $privateKeyFile = null,
        ?string $certificateChainFile = null
    ) : self{
        return self::tls(
            self::readOptionalFile($rootCertificatesFile),
            self::readOptionalFile($privateKeyFile),
            self::readOptionalFile($certificateChainFile)
        );
    }

    /**
     * @internal
     * @return array{mode: string, rootCertificates: ?string, privateKey: ?string, certificateChain: ?string}
     */
    public function export() : array{
        return [
            'mode' => $this->mode,
            'rootCertificates' => $this->rootCertificates,
            'privateKey' => $this->privateKey,
            'certificateChain' => $this->certificateChain,
        ];
    }

    private static function readOptionalFile(?string $path) : ?string{
        if($path === null){
            return null;
        }

        $contents = @file_get_contents($path);
        if($contents === false){
            throw new RuntimeException("Unable to read TLS file: {$path}");
        }
        return $contents;
    }
}
