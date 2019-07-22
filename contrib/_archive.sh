#!/bin/bash

set -e
set -u

cd $(dirname $0)/..

rm -rf dist

mkdir -p dist

cp -rf contrib/logtubed.yml contrib/logtubed.service dist/

GO111MODULE=on CGO_ENABLED=0 go build -mod vendor -ldflags "-X main.Version=`date -u +%Y-%m-%d_%H-%M-%S`" -o dist/logtubed
cd tools/logtubemon && GO111MODULE=on CGO_ENABLED=0 go build -mod vendor -o ../../dist/logtubemon && cd ../..

cp -rf contrib/_install.sh dist/install.sh

tar czf logtubed-dist-linux-amd64.tar.gz dist

rm -rf dist
