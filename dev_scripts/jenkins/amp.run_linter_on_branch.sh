#!/bin/bash -e

# """
# - No conda env is built, but we rely on `p1_develop.daily_build` being already
#  build.
# - This script runs the linter on a git branch.
# """


CONDA_ENV="amp_develop.daily_build"
DEV_SCRIPTS_DIR="./dev_scripts_p1"

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Init.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

CMD="source ./dev_scripts/jenkins/amp.jenkins_helpers.sh"
echo "+ $CMD"
eval $CMD

source_scripts

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Setenv.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

setenv "$AMP/dev_scripts/setenv_amp.sh" $CONDA_ENV

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Run linter.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

linter.py -b --post_check

