#!/bin/bash -xe

# """
# - Build conda env
# - Run the fast tests with coverage
# """

EXEC_NAME=`basename "$0"`
AMP="."
CONDA_ENV="amp_develop.build_clean_env.run_fast_coverage_tests"

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

prepare_to_build_env "base"

# From dev_scripts/create_conda.sh
CMD="$CREATE_CONDA_PY --env_name $CONDA_ENV --req_file dev_scripts/install/requirements/develop.yaml --delete_env_if_exists -v $VERB"
execute $CMD

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Setenv.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

## Config environment.
#echo "$EXEC_NAME: source dev_scripts/setenv.sh -e $CONDA_ENV"
#source dev_scripts/setenv.sh -e $CONDA_ENV
#
## Check conda env.
#CMD="print_conda_packages.py"
#frame "$EXEC_NAME: $CMD"
#execute $CMD
#
#CMD="check_develop_packages.py"
#frame "$EXEC_NAME: $CMD"
#execute $CMD

setenv $CONDA_ENV

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Run.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

# Run tests.
OPTS='--test fast --coverage'
CMD="$RUN_TESTS_PY $OPTS --jenkins -v $VERB"
execute $CMD
