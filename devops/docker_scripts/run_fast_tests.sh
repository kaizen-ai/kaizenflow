#!/usr/bin/env bash

set -e

USER_OPTS="$*"

OPTS="-vv -rpa"
#OPTS="$OPTS --log-cli-level=INFO"

SKIPPED_TESTS="not slow and \
    not superslow and \
    not broken_deps and \
    not need_data_dir and \
    not not_docker"

# Run tests.
cmd="pytest ${OPTS} -m '${SKIPPED_TESTS}' ${USER_OPTS}"
echo "> cmd=$cmd"
eval $cmd
