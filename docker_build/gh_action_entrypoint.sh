#!/usr/bin/env bash

source docker_build/entrypoint/patch_environment_variables.sh
source docker_build/entrypoint/gh_action_aws_credentials.sh

mount -a

source ~/.bashrc
conda activate venv