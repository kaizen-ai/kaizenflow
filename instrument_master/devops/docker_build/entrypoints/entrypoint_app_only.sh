#!/bin/bash

# This is used for docker_bash when we don't want to bring up the DB.

set -e

echo "STAGE: $STAGE"
echo "POSTGRES_HOST: $POSTGRES_HOST"
echo "POSTGRES_PORT: $POSTGRES_PORT"
echo "PostgreSQL will not be started"

umask 000

source ~/.bashrc

eval "$@"
