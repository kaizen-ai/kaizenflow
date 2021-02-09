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

# TODO: remove this line once the dependency will be added to the image.
# ./edgar/compose/install_postgres_client12.sh

# TODO(plyq): Change path once amp image will be ready
# ./amp/vendors_amp/kibot/compose/init_local_db.sh
 
./docker_build/entrypoint.sh "$@"
