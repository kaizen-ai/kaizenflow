#!/bin/bash -e

# Run tests.
OPTS="--test slow --coverage -v $VERBOSITY"
run_tests $AMP $OPTS
