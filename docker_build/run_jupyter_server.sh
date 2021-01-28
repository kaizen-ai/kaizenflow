#!/usr/bin/env bash

source ./docker_build/entrypoint.sh
jupyter notebook --ip=* --port=${J_PORT} --allow-root