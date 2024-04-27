#!/bin/bash -e

IMAGE_NAME="myhbase"
CONTAINER_NAME='hbase-docker'
data_dir='$PWD/data'
rm -rf "$data_dir"
mkdir -p "$data_dir"

docker run --name=${CONTAINER_NAME} -h ${CONTAINER_NAME} -d -P -v "$data_dir:/data" "$IMAGE_NAME"