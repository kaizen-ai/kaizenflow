#!/bin/bash -xe

# TODO(gp): -> "amp_develop".
CONDA_ENV="amp_develop"
# TODO(gp): Remove this.
#REQ_FILE="dev_scripts/install/requirements/develop.txt"
REQ_FILE="dev_scripts/install/requirements/develop.yaml"
create_conda.py \
  --env_name $CONDA_ENV \
  --req_file $REQ_FILE \
  --delete_env_if_exists
