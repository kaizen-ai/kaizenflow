#!/bin/bash -xe

IMAGE_NAME=defi
docker build --progress plain -t $IMAGE_NAME .
