#!/bin/bash -xe

CONDA_ENV="amp_develop"
dev_scripts/install/create_conda.py \
  --env_name $CONDA_ENV \
  --req_file dev_scripts/install/requirements/develop.yaml \
  --delete_env_if_exists
