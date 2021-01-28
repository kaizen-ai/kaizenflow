#!/usr/bin/env bash
#docker_build/entrypoint/run_make.sh
cd amp
source docker_build/entrypoint/patch_environment_variables.sh
cd ..
source docker_build/entrypoint/aws_credentials.sh
source docker_build/entrypoint/patch_environment_variables.sh
source docker_build/entrypoint/google_secret_credentials.sh

mount -a

source ~/.bashrc
conda activate venv
# To make possible work with files outside a container
umask 000

./docker_build/test/test_mount_fsx.sh
./docker_build/test/test_mount_s3.sh
./docker_build/test/test_volumes.sh

exec "$@"
