rem COMMIT ?= "$(shell git describe --long --dirty --always --match "" || true)"
rem CLEAN_COMMIT ?= "$(shell git describe --long --always --match "" || true)"
rem COMMIT_TIME ?= "$(shell git show -s --format=%ct $(CLEAN_COMMIT) || true)"
rem LDFLAGS ?= -s -w -X github.com/ethersphere/bee.commit="$(COMMIT)" -X github.com/ethersphere/bee.commitTime="$(COMMIT_TIME)"
echo remember to git describe --long --dirty --always --match "" to fix commit string!
rem git describe --tags --abbrev=0 | cut -c2- >version.txt
git describe --tags --abbrev=0 >version.txt
set /p version=<version.txt
git describe --long --dirty --always --match "" >commitHash.txt
set /p commitHash=<commitHash.txt
git describe --long --always --match "" >commitClean.txt
set /p commitClean=<commitClean.txt
git show -s --format=%%ct %commitClean% >commitTime.txt
set /p commitTime=<commitTime.txt

grep "^  version:" openapi/Swarm.yaml | grep -o "[0-9]*\.[0-9]*\.[0-9]*" >apiVersion.txt
set /p apiVersion=<apiVersion.txt
grep "^  version:" openapi/SwarmDebug.yaml | grep -o "[0-9]*\.[0-9]*\.[0-9]*" >debugVersion.txt
set /p debugVersion=<debugVersion.txt

echo %version% %commitHash%
echo APIs: %apiVersion% %debugVersion%

set GOOS=linux
set GOARCH=amd64
rem https://github.com/golang/go/issues/45453
rem set GOAMD64=v???

set CGO_ENABLED=0
set BATCHFACTOR_OVERRIDE_PUBLIC=5
set REACHABILITY_OVERRIDE_PUBLIC=false

go build -trimpath -ldflags ^"-s -w^
 -X github.com/ethersphere/bee/v2.version="%version%"^
 -X github.com/ethersphere/bee/v2.commitHash="%commitHash%"^
 -X github.com/ethersphere/bee/v2.commitTime="%commitTime%"^
 -X github.com/ethersphere/bee/v2/pkg/api.Version="%apiVersion%"^
 -X github.com/ethersphere/bee/v2/pkg/api.DebugVersion="%debugVersion%"^
 -X github.com/ethersphere/bee/v2/pkg/p2p/libp2p.reachabilityOverridePublic="%REACHABILITY_OVERRIDE_PUBLIC%"^
 -X github.com/ethersphere/bee/v2/pkg/postage/listener.batchFactorOverridePublic="%BATCHFACTOR_OVERRIDE_PUBLIC%"^" ^
 -o dist/bee-amd64-%version%-%commitHash% ./cmd/bee
