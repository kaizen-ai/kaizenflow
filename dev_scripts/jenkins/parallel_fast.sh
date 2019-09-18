#!/bin/bash -xe

# ```
# - (No conda env build)
# - Run tests
#   - fast
#   - parallel
# ```

VERB=DEBUG
ENV_NAME=develop
NUM_CPUS=4

source ~/.bashrc

# Config.
source dev_scripts/setenv.sh -e $ENV_NAME

# Run tests.
dev_scripts/run_tests.py --test fast --num_cpus $NUM_CPUS --jenkins -v $VERB
