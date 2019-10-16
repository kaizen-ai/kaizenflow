#!/bin/bash -xe

# """
# - Collect tests
# """

VERB=DEBUG
ENV_NAME=develop

# Config.
source ~/.bashrc
source dev_scripts/setenv.sh -e $ENV_NAME

# Run.
pytest --collect-only -q -rs
pytest --collect-only -qq -rs
