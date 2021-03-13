#!/usr/bin/env bash
set -ex

SKIPPED_TESTS="not slow and not superslow and not broken_deps and not need_data_dir and not not_docker"
OPTS="-vv -rpa"

# Run tests.
pytest --log-cli-level=ERROR ${OPTS} -m "${SKIPPED_TESTS}"
