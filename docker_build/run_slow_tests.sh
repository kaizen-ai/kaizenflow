#!/usr/bin/env bash
set -ex

source ./docker_build/entrypoint.sh

SKIPPED_TESTS="slow and not superslow and not broken_deps and not need_data_dir and not not_docker"
OPTS="-vv -rpa"

# Collect tests without executing them.
#pytest --collect-only ${OPTS} -m "${SKIPPED_TESTS}"

# Run tests.
pytest --log-cli-level=ERROR ${OPTS} -m "${SKIPPED_TESTS}"
