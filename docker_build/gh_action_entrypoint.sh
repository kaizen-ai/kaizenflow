#!/usr/bin/env bash

#docker_build/entrypoint/run_make.sh
cd amp
source docker_build/entrypoint/patch_environment_variables.sh

cd ..
source docker_build/entrypoint/gh_action_aws_credentials.sh
source docker_build/entrypoint/patch_environment_variables.sh

mount -a

source ~/.bashrc
conda activate venv