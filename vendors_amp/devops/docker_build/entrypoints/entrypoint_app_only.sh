#!/bin/bash

echo "STAGE: $STAGE"
echo "POSTGRES_HOST: $POSTGRES_HOST"
echo "POSTGRES_PORT: $POSTGRES_PORT"
echo "PostgreSQL will not be started..."

umask 000

eval "$@"
