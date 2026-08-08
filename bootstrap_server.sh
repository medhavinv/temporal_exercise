#!/usr/bin/env bash
# Get a Temporal CLI (which embeds a full dev server) into ./bin/temporal.
#
# Path 1 is the normal one. Path 2 exists because some sandboxed and corporate
# environments block temporal.download and github.com but allow the Go module
# proxy -- the CLI is a Go module, so it can be built from source instead.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
mkdir -p bin

if [ -x bin/temporal ]; then
  echo "bin/temporal already present: $(./bin/temporal --version)"
  exit 0
fi

if command -v temporal >/dev/null 2>&1; then
  ln -sf "$(command -v temporal)" bin/temporal
  echo "linked the temporal already on your PATH: $(./bin/temporal --version)"
  exit 0
fi

echo "== path 1: official installer =="
if curl -sSf --max-time 60 https://temporal.download/cli.sh 2>/dev/null | sh -s -- --dir "$PWD/bin" 2>/dev/null; then
  [ -x bin/temporal ] && { echo "installed: $(./bin/temporal --version)"; exit 0; }
fi
echo "   unavailable (blocked or offline), falling back"

echo "== path 2: build from the Go module proxy =="
command -v go >/dev/null 2>&1 || {
  echo "go is not installed, and the installer is unreachable." >&2
  echo "Install the Temporal CLI manually: https://docs.temporal.io/cli" >&2
  exit 1
}

# v1.5.0 is the newest release whose go.mod has no local `replace` directives,
# so it builds straight out of the module cache. Later versions reference a
# ./cliext sibling module that is not published to the proxy.
VERSION=${TEMPORAL_CLI_VERSION:-v1.5.0}
BUILD=$(mktemp -d)
trap 'rm -rf "$BUILD"' EXIT

export GOPROXY=${GOPROXY:-https://proxy.golang.org,direct}
export GOFLAGS=-mod=mod

echo "   downloading github.com/temporalio/cli@$VERSION"
( cd "$BUILD" && go mod init bootstrap >/dev/null 2>&1 || true
  go mod download "github.com/temporalio/cli@$VERSION" )

SRC="$(go env GOMODCACHE)/github.com/temporalio/cli@$VERSION"
[ -d "$SRC" ] || { echo "module download failed" >&2; exit 1; }

echo "   building (a few minutes the first time)"
cp -r "$SRC" "$BUILD/cli-src"
chmod -R u+w "$BUILD/cli-src"
( cd "$BUILD/cli-src" && go build -o "$OLDPWD/bin/temporal" ./cmd/temporal )

echo "built: $(./bin/temporal --version)"
