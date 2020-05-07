#!/bin/bash -e

OPTS="--test slow -v $VERBOSITY"
run_tests $AMP $OPTS

