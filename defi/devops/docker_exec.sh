#!/bin/bash -xe

IMAGE_NAME=defi
CONTAINER_ID=$(docker container ls | grep $IMAGE_NAME | awk '{print $1}')
OPTS="--user $(id -u):$(id -g)"
#OPTS=""
docker exec $OPTS -it $CONTAINER_ID bash
