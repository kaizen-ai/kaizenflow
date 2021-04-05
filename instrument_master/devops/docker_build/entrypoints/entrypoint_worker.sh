#!/bin/bash

set -e

env
cd /app
pwd

# Nasty code
# Some stuff to make work aws cli.
# Another part of this nasty trick in instrument_master/devops/docker_build/im_db_loader_worker.dev.Dockerfile
if sudo test -d "/root/.aws" ; then
echo "Copy aws credentials."
sudo cp -r /root/.aws /home/airflow/
sudo chown -R airflow:airflow /home/airflow/.aws
else
echo "Skip copy aws credentials."
fi
# End of nasty code

postgres_ready() {
  pg_isready -d $POSTGRES_DB -p $POSTGRES_PORT -h $POSTGRES_HOST
}

echo "STAGE: $STAGE"
echo "POSTGRES_HOST: $POSTGRES_HOST"
echo "POSTGRES_PORT: $POSTGRES_PORT"

until postgres_ready; do
  >&2 echo 'Waiting for PostgreSQL to become available...'
  sleep 1
done
>&2 echo 'PostgreSQL is available'

umask 000

# source ~/.bashrc

export PYTHONPATH=/app:$PYTHONPATH
echo "PYTHONPATH=$PYTHONPATH"

# Initialize the DB.
./instrument_master/devops/docker_scripts/init_im_db.py --db $POSTGRES_DB

eval "$@"
