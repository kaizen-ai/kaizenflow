#!/bin/bash -xe

# TODO(gp): -> "amp_develop".
CONDA_ENV="develop"
create_conda.py \
  --env_name $CONDA_ENV \
  --req_file dev_scripts/install/requirements/develop.yaml \
  --delete_env_if_exists
