#!/bin/bash -e

# Find the name of the container.
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
DOCKER_NAME="$SCRIPT_DIR/docker_name.sh"
if [[ ! -e $SCRIPT_DIR ]]; then
    echo "Can't find $DOCKER_NAME"
    exit -1
fi;
source $DOCKER_NAME

docker image ls $FULL_IMAGE_NAME

CONTAINER_NAME=$IMAGE_NAME
OPTS="--user $(id -u):$(id -g)"
# TODO(gp): Not sure --net bridge is needed.
docker run \
    --rm -ti \
    $OPTS \
    --net bridge \
    --name $CONTAINER_NAME \
    -p 5920:5920 -p 3001:3000 -p 7545:7545 -p 8545:8545 -p 8889:8888 \
    -v $(pwd):/data \
    $FULL_IMAGE_NAME
