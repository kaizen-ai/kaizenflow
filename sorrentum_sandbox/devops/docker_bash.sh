#!/bin/bash -e
#
# Execute bash in the Sorrentum container.
#

IMAGE_NAME="airflow_cont"

docker exec \
    -ti \
    $IMAGE_NAME \
    bash
