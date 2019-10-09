#!/bin/bash -xe

VERB=DEBUG
ENV_NAME=develop.clean_build.run_slow_tests_coverage

source ~/.bashrc

# Create a fresh conda install.
export PYTHONPATH=""
conda activate base
source dev_scripts/setenv.sh -e base
env
dev_scripts/install/create_conda.py --delete_env_if_exists --env_name $ENV_NAME -v $VERB

# Config.
source dev_scripts/setenv.sh -e $ENV_NAME

# Run tests.
OPTS='--coverage'
dev_scripts/run_tests.py --test slow --jenkins $OPTS -v $VERB
