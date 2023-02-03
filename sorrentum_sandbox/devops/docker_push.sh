#!/bin/bash -e

IMAGE_NAME="sorrentum"
REPO_NAME="sorrentum"

docker login --username $REPO_NAME --password-stdin <~/.docker/passwd.${REPO_NAME}.txt

docker push docker.io/$REPO_NAME/$IMAGE_NAME
