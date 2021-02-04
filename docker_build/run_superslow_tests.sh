#!/usr/bin/env bash

source ./docker_build/entrypoint.sh

conda list

OPTS='-vv -rpa  -m "superslow and not slow and not broken_deps and not need_data_dir and not not_docker"'

# Collect without execution
pytest --co $OPTS

# Run tests
pytest --log-cli-level=ERROR $OPTS
