#!/usr/bin/env bash

source ./docker_build/entrypoint.sh
conda list
# Collect without execution
pytest --co -vv -rpa  -m "slow and not superslow and not broken_deps and not need_data_dir and not not_docker"
# Run tests
pytest --log-cli-level=ERROR -vv -rpa  -m "slow and not superslow and not broken_deps and not need_data_dir and not not_docker"
