#!/usr/bin/env bash

set -e

FILE_NAME="devops/docker_build/entrypoint.sh"
echo "##> $FILE_NAME"

echo "# Activate environment"
source /${ENV_NAME}/bin/activate

# TODO(gp): Since we execute bash in eval we lose this config.
set -o vi

# We use ~/.aws to pass the credentials.
# TODO(gp): -> set_aws_env_vars.sh
#source devops/docker_build/entrypoint/aws_credentials.sh

# TODO(gp): -> set_env_vars.sh
source devops/docker_build/entrypoint/patch_environment_variables.sh

mount -a || true

echo "UID="$(id -u)
echo "GID="$(id -g)

# Allow working with files outside a container.
#umask 000

# TODO(gp): Merge all this in a single script `devops/docker_build/test_setup.sh`.
./devops/docker_build/test/test_mount_fsx.sh
./devops/docker_build/test/test_mount_s3.sh
./devops/docker_build/test/test_volumes.sh

echo "AWS_ACCESS_KEY_ID='$AWS_ACCESS_KEY_ID'"
echo "AWS_SECRET_ACCESS_KEY='***'"
echo "AWS_DEFAULT_REGION='$AWS_DEFAULT_REGION'"

echo "CONTAINER_VERSION='$CONTAINER_VERSION'"
echo "BUILD_TAG='$BUILD_TAG'"

echo "which python: "$(which python)
echo "check pandas package: "$(python -c "import pandas; print(pandas)")

# CmampTask16: Add env var.
export HOME=/app/home

echo "PATH=$PATH"
echo "PYTHONPATH=$PYTHONPATH"
echo "entrypoint.sh: '$@'"

# TODO(gp): eval seems to be more general, but it creates a new executable.
eval "$@"
