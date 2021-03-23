#!/bin/bash

postgres_ready() {
  pg_isready -d $POSTGRES_DB -p $POSTGRES_PORT -h $POSTGRES_HOST
}

until postgres_ready; do
  >&2 echo 'Waiting for PostgreSQL to become available...'
  sleep 1
done
>&2 echo 'PostgreSQL is available'
echo "STAGE: $STAGE"
echo "POSTGRES_HOST: $POSTGRES_HOST"
echo "POSTGRES_PORT: $POSTGRES_PORT"

./devops/docker_build/entrypoint.sh "$@"
