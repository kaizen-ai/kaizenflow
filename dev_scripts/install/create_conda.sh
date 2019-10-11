#!/bin/bash -xe

# TODO(gp): -> "amp_develop".
#CONDA_ENV="amp_develop"
CONDA_ENV="develop"
if [[ 1 == 1 ]]; then
  # txt flow.
  # TODO(gp): Remove this.
  create_conda.py \
    --env_name $CONDA_ENV \
    --req_file dev_scripts/install/requirements/develop.txt \
    --delete_env_if_exists
else
  # yaml flow.
  create_conda.py \
    --env_name $CONDA_ENV \
    --req_file dev_scripts/install/requirements/develop.yaml \
    --yaml \
    --delete_env_if_exists
fi;
