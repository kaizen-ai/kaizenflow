#!/bin/bash -e

CONDA_ENV="develop"
create_conda.py --env_name $CONDA_ENV \
  --req_file dev_scripts/install/requirements/develop.txt \
  --delete_env_if_exists"
