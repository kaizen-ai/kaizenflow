#!/bin/bash -e
#
# Delete all the Sorrentum images.
#

REPO_NAME="sorrentum"
IMAGE_NAME="sorrentum"
FULL_IMAGE_NAME=$IMAGE_NAME/$REPO_NAME

# Print Sorrentum images.
docker image ls | grep $FULL_IMAGE_NAME

# Remove images.
docker image ls | grep $FULL_IMAGE_NAME | awk '{print $1}' | xargs -n 1 -t docker image rm -f

# Print all images.
docker image ls
