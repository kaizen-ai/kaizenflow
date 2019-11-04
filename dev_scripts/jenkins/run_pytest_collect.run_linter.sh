#!/bin/bash -xe

# """
# - Run linter on the entire tree.
# """

EXEC_NAME=`basename "$0"`
AMP="."
CONDA_ENV="amp_develop.daily_build"

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Init.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

CMD="source ~/.bashrc"
echo "+ $CMD"
eval $CMD

CMD="source $AMP/dev_scripts/helpers.sh"
echo "+ $CMD"
eval $CMD

CMD="source $AMP/dev_scripts/jenkins/jenkins_helpers.sh"
echo "+ $CMD"
eval $CMD

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Setenv.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

setenv $CONDA_ENV

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Run tests.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

# Collect tests.
CMD="pytest --collect-only -q -rs"
execute $CMD

CMD="pytest --collect-only -qq -rs"
execute $CMD

# Run (ignoring the rc).
CMD="linter.py -d . --jenkins --num_threads serial"
execute $CMD
