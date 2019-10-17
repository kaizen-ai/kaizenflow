#!/bin/bash -xe

# TODO(gp): -> run_parallel_fast_test.sh

# """
# - (No conda env build)
# - Run tests
#   - fast
#   - parallel
# """

EXEC_NAME=`basename "$0"`
AMP="."
CONDA_ENV="develop"
VERB="DEBUG"

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Init.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

# Init.
echo "$EXEC_NAME: source ~/.bashrc"
source ~/.bashrc
# TODO(gp): This used to be needed.
#export PYTHONPATH=""

echo "$EXEC_NAME: source $AMP/dev_scripts/helpers.sh"
source $AMP/dev_scripts/helpers.sh

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Setenv.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

# Config environment.
echo "$EXEC_NAME: source dev_scripts/setenv.sh -e $CONDA_ENV"
source dev_scripts/setenv.sh -e $CONDA_ENV
# Print env.
echo "$EXEC_NAME: env"
env

# Check conda env.
CMD="print_conda_packages.py"
frame "$EXEC_NAME: $CMD"
execute $CMD

CMD="check_develop_packages.py"
frame "$EXEC_NAME: $CMD"
execute $CMD

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Run.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

# Run tests.
OPTS="--test fast --num_cpus -1" 
CMD="dev_scripts/run_tests.py $OPTS --jenkins -v $VERB"
frame "$EXEC_NAME: $CMD"
execute $CMD
