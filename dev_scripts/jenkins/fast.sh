#!/bin/bash -e

# TODO(gp): -> run_fast_tests.sh

# """
# - No conda env is built, but we rely on `develop` being already build
# - Run the fast tests
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
OPTS="--test fast"
CMD="dev_scripts/run_tests.py $OPTS --jenkins -v $VERB"
frame "$EXEC_NAME: $CMD"
execute $CMD
