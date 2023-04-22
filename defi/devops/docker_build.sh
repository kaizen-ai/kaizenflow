#!/bin/bash -xe

REPO_NAME=sorrentum
IMAGE_NAME=defi
FULL_IMAGE_NAME=$REPO_NAME/$IMAGE_NAME

docker build --progress plain -t $FULL_IMAGE_NAME .
