#!/bin/bash -xe

REPO_NAME=sorrentum
IMAGE_NAME=defi
FULL_IMAGE_NAME=$REPO_NAME/$IMAGE_NAME

docker image ls $FULL_IMAGE_NAME

CONTAINER_NAME=$IMAGE_NAME
OPTS="--user $(id -u):$(id -g)"
# TODO(gp): Not sure --net bridge is needed.
docker run $OPTS --rm -ti --net bridge \
    --name $CONTAINER_NAME \
    -p 5920:5920 -p 3001:3000 -p 7545:7545 -p 8545:8545 -p 8889:8888 \
    -v $(pwd):/data $IMAGE_NAME
