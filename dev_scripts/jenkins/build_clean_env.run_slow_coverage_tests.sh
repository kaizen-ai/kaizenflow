#!/bin/bash -e

# """
# - Build conda env
# - Run the slow tests with coverage
# """

EXEC_NAME=`basename "$0"`
AMP="."
CONDA_ENV="amp_develop.build_clean_env.run_slow_coverage_tests"

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
# Build env.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

prepare_to_build_env

# From dev_scripts/create_conda.sh
CMD="$CREATE_CONDA_PY --env_name $CONDA_ENV --req_file dev_scripts/install/requirements/develop.yaml --delete_env_if_exists -v $VERB"
execute $CMD

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Setenv.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

setenv $CONDA_ENV

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Run.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

# Run tests.
OPTS='--test slow --coverage'
CMD="$RUN_TESTS_PY $OPTS --jenkins -v $VERB"
execute $CMD
