#!/bin/bash -e

# """
# Build "amp_develop" conda env from scratch.
# """

CONDA_ENV="develop"
EXEC_NAME="dev_scripts/jenkins/create_conda.$CONDA_ENV.sh"
AMP="."

echo "$EXEC_NAME: source ~/.bashrc"
source ~/.bashrc
export PYTHONPATH=""

# Activate conda.
echo "$EXEC_NAME: conda activate base"
conda activate base

# Configure environment.
echo "$EXEC_NAME: source $AMP/dev_scripts/setenv.sh -e base"
source $AMP/dev_scripts/setenv.sh -e base

echo "$EXEC_NAME: env"
env

echo "$EXEC_NAME: source $AMP/dev_scripts/helpers.sh"
source $AMP/dev_scripts/helpers.sh

# TODO(gp): dev_scripts/create_conda.sh
CMD="create_conda.py --env_name $CONDA_ENV --req_file dev_scripts/install/requirements/develop.txt --delete_env_if_exists"
frame "$EXEC_NAME: $CMD"
execute $CMD

echo "$EXEC_NAME: source dev_scripts/setenv.sh -e $CONDA_ENV"
source dev_scripts/setenv.sh -e $CONDA_ENV

CMD="print_conda_packages.py"
frame "$EXEC_NAME: $CMD"
execute $CMD

CMD="check_develop_packages.py"
frame "$EXEC_NAME: $CMD"
execute $CMD
