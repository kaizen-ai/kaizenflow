#!/bin/bash -xe

# TODO(gp): -> docker_clean_postgres.sh

docker container ls | grep postgres:13 | awk '{print $1}' | xargs docker container rm --force

docker volume ls | \grep postgres | awk '{print $2}' | xargs docker volume rm

docker network ls | grep postgres | awk '{ print $1 }' | xargs docker network rm
