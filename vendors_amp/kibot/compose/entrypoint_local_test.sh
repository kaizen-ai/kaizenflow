#!/bin/bash

postgres_ready() {
  pg_isready -d $POSTGRES_DB -p $POSTGRES_PORT -h $POSTGRES_HOST
}

# TODO: remove this line once the dependency will be added to the image.
apt -y install postgresql-client

until postgres_ready; do
  >&2 echo 'Waiting for PostgreSQL to become available...'
  sleep 1
done
>&2 echo 'PostgreSQL is available'
echo "STAGE: $STAGE"
echo "POSTGRES_HOST: $POSTGRES_HOST"
echo "POSTGRES_PORT: $POSTGRES_PORT"

./docker_build/entrypoint.sh "$@"
