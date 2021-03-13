#!/usr/bin/env bash

source docker_build/entrypoint/patch_environment_variables.sh
source docker_build/entrypoint/gh_action_aws_credentials.sh

mount -a

source ~/.bashrc
conda activate venv

# Allow working with files outside a container.
umask 000

./docker_build/test/test_mount_fsx.sh
./docker_build/test/test_mount_s3.sh

exec "$@"
