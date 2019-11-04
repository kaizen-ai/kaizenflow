#!/bin/bash -e

# """
# - Build "amp_develop" conda env from scratch.
# """

EXEC_NAME=`basename "$0"`
AMP="."
CONDA_ENV="amp_develop.daily_build"
VERB="DEBUG"
CREATE_CONDA_PY="./dev_scripts/install/create_conda.py"

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Init.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
echo "$EXEC_NAME: source ~/.bashrc"
source ~/.bashrc
## TODO(gp): This used to be needed.
##export PYTHONPATH=""

CMD="source $AMP/dev_scripts/helpers.sh"
echo "$EXEC_NAME: $CMD"
eval $CMD

CMD="source $AMP/dev_scripts/jenkins/jenkins_helpers.sh"
echo "$EXEC_NAME: $CMD"
source $CMD

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Build env.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

## Activate conda base environment.
#CMD="conda activate base"
#execute $CMD
#
### Configure base environment.
##echo "$EXEC_NAME: source $AMP/dev_scripts/setenv_amp.sh -e base"
##source $AMP/dev_scripts/setenv.sh -e base
#
## Print env.
#echo "$EXEC_NAME: env"
#env

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
