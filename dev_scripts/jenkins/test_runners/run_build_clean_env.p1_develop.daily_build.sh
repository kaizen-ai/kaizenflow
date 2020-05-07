#!/bin/bash -e

# """
# - Build "p1_develop" conda env from scratch.
# """

CONDA_ENV="p1_develop.daily_build"
DEV_SCRIPTS_DIR="./dev_scripts_p1"

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Init.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

CMD="source $DEV_SCRIPTS_DIR/jenkins/p1.jenkins_helpers.sh"
echo "+ $CMD"
eval $CMD

source_scripts

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Build env.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

prepare_to_build_env

# From $DEV_SCRIPTS_DIR/create_conda.sh
OPTS="--env_name $CONDA_ENV $CREATE_CONDA_OPTS -v $VERBOSITY"
create_conda $AMP $OPTS
