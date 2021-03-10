#!/usr/bin/env bash

export PYTHONPATH=$PYTHONPATH:/amp

# Allow working with files outside a container.
umask 000

exec "$@"
