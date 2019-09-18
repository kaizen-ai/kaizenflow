#!/bin/bash -xe

VERB=DEBUG
ENV_NAME=develop

source ~/.bashrc

# Create a fresh conda install.
dev_scripts/create_conda.py --delete_env_if_exists --env_name $ENV_NAME -v $VERB

# Config.
source dev_scripts/setenv.sh -e $ENV_NAME

# Run tests.
OPTS='--coverage'
dev_scripts/run_tests.py --test slow --jenkins $OPTS -v $VERB
