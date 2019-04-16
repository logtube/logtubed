#!/bin/bash

docker run -ti --rm -v $(pwd -P)/$(basename "$1"):/go/src/github.com/logtube/logtubed golang:1.12 /go/src/github.com/logtube/logtubed/scripts/_archive.sh
