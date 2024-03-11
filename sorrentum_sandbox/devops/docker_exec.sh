#!/bin/bash -e

IMAGE_NAME="devops-airflow-webserver-1"

CONTAINER_ID=$(docker container ls | grep $IMAGE_NAME | awk '{print $1}')
docker exec -it $CONTAINER_ID bash
