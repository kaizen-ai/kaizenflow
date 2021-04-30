#!/usr/bin/env bash

set -e

USER_OPTS="$*"

OPTS="-vv -rpa"

SKIPPED_TESTS="slow and \
not superslow"

# Run tests.
cmd="pytest ${OPTS} -m '${SKIPPED_TESTS}' ${USER_OPTS}"
echo "> cmd=$cmd"
eval $cmd
