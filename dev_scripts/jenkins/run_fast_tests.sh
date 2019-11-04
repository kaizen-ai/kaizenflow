#!/bin/bash -e

# """
# - No conda env is built, but we rely on `develop` being already build
# - Run the fast tests
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

OPTS="--test fast"
CMD="$RUN_TESTS_PY $OPTS --jenkins -v $VERB"
execute $CMD
