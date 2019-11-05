#!/bin/bash -xe

# """
# - Build conda env
# - Run the fast tests with coverage
# """

EXEC_NAME=`basename "$0"`
CONDA_ENV="amp_develop.build_clean_env.run_fast_coverage_tests"

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Init.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

CMD="source ./dev_scripts/jenkins/amp.jenkins_helpers.sh"
echo "+ $CMD"
eval $CMD

source_scripts

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Build env.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

prepare_to_build_env "base"

# From dev_scripts/create_conda.sh
OPTS="--env_name $CONDA_ENV --req_file dev_scripts/install/requirements/develop.yaml --delete_env_if_exists -v $VERB"
create_conda $AMP $OPTS

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Setenv.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

setenv "setenv_amp.sh" $CONDA_ENV

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Run.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

# Run tests.
OPTS="--test fast --coverage -v $VERB"
run_tests $AMP $OPTS
