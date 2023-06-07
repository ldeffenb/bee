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
echo %version% %commitHash%

set CGO_ENABLED=0
set BATCHFACTOR_OVERRIDE_PUBLIC=5
set REACHABILITY_OVERRIDE_PUBLIC=false

go build -trimpath -ldflags ^"-s -w^
 -X github.com/ethersphere/bee.version="%version%"^
 -X github.com/ethersphere/bee.commitHash="%commitHash%"^
 -X github.com/ethersphere/bee.commitTime="%commitTime%"^
 -X github.com/ethersphere/bee/pkg/p2p/libp2p.reachabilityOverridePublic="%REACHABILITY_OVERRIDE_PUBLIC%" -X github.com/ethersphere/bee/pkg/postage/listener.batchFactorOverridePublic="%BATCHFACTOR_OVERRIDE_PUBLIC%"^" ^
 -o dist/bee-%version%-%commitHash%.exe ./cmd/bee
