#!/bin/bash

set -e
set -u

cd $(dirname $0)/..

rm -rf dist

mkdir -p dist

cp -rf contrib/logtubed.yml contrib/logtubed.service dist/

GO111MODULE=off CGO_ENABLED=0 go build -ldflags "-X main.Version=`date -u +%Y-%m-%d_%H-%M-%S`" -o dist/logtubed
cd cmd/logtubemon && GO111MODULE=off CGO_ENABLED=0 go build -o ../../dist/logtubemon && cd ../..
cd cmd/logtubedash && GO111MODULE=off CGO_ENABLED=0 go build -o ../../dist/logtubedash && cd ../..

cp -rf contrib/_install.sh dist/install.sh

tar czf logtubed-dist-linux-amd64.tar.gz dist

rm -rf dist
