#!/usr/bin/env bash

source ./docker_build/gh_action_entrypoint.sh
pytest -vv -rpa  -m "slow and not superslow and not broken_deps and not need_data_dir and not not_docker"
