#!/bin/bash -e

REPO_NAME="sorrentum"
IMAGE_NAME="sorrentum"
FULL_IMAGE_NAME=$IMAGE_NAME/$REPO_NAME

docker image ls | grep $FULL_IMAGE_NAME
docker image ls | grep $FULL_IMAGE_NAME | awk '{print $1}' | xargs -n 1 -t docker image rm -f
docker image ls
