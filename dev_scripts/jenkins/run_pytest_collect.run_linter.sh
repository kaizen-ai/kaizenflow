#!/bin/bash -xe

# """
# - Run linter on the entire tree.
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

# Collect tests.
CMD="pytest --collect-only -q -rs"
frame "$EXEC_NAME: $CMD"
execute $CMD

CMD="pytest --collect-only -qq -rs"
frame "$EXEC_NAME: $CMD"
execute $CMD

# Run (ignoring the rc).
CMD="linter.py -d . --jenkins --num_threads serial"
frame "$EXEC_NAME: $CMD"
execute $CMD
