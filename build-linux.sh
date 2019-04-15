#!/bin/bash

GO111MODULE=off CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-X main.Version=`date -u +%Y-%m-%d_%H-%M-%S`" -o logtubed-linux-amd64
