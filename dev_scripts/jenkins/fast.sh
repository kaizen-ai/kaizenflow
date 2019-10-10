#!/bin/bash -xe

# """
# - (No conda env build)
# - Run the fast tests
# """

VERB=DEBUG
ENV_NAME=develop

source ~/.bashrc

# Config.
source dev_scripts/setenv.sh -e $ENV_NAME

# Run tests.
dev_scripts/run_tests.py --test fast --jenkins -v $VERB
