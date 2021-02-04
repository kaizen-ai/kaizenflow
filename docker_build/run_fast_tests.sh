#!/usr/bin/env bash

# TODO(Sergey): Why not calling `source ./docker_build/entrypoint.sh`?
# The other scripts have it.

conda list

OPTS='-vv -rpa -m "not superslow and not slow and not broken_deps and not need_data_dir and not not_docker"'

# Collect tests without executing them.
pytest --co $OPTS

# Run tests.
pytest --log-cli-level=ERROR $OPTS
