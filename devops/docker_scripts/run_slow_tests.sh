#!/usr/bin/env bash

set -e

OPTS="-vv -rpa --log-cli-level=INFO"

SKIPPED_TESTS="slow and not superslow and not broken_deps and not need_data_dir and not not_docker"

# Run tests.
cmd="pytest ${OPTS} -m '${SKIPPED_TESTS}'"
echo "> cmd=$cmd"
eval $cmd
