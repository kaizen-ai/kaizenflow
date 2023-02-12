#!/bin/bash -xe
#
# Kill all the defi containers.
#

CONTAINER_NAME=defi
docker container ls -a | grep $CONTAINER_NAME | awk '{print $1}' | xargs docker container rm -f
