#!/usr/bin/env bash

set -e

FILE_NAME="devops/docker_build/entrypoint.sh"
echo "##> $FILE_NAME"

echo "# Activate environment"
source /${ENV_NAME}/bin/activate

# TODO(gp): Since we execute bash in eval we lose this config.
set -o vi

# Add env var to allow to run with `docker --user ...` (see CmampTask16).
export HOME=/app/home
echo "HOME='$HOME'"
echo "UID="$(id -u)
echo "GID="$(id -g)

# We use ~/.aws and the env vars to pass the AWS credentials.
# TODO(gp): Remove this script.
# TODO(gp): -> set_aws_env_vars.sh
#source devops/docker_build/entrypoint/aws_credentials.sh

# TODO(gp): -> set_env_vars.sh
source devops/docker_build/entrypoint/patch_environment_variables.sh

#mount -a || true

# Allow working with files outside a container.
#umask 000

# TODO(gp): Merge all this in a single script `devops/docker_build/test_setup.sh`.
./devops/docker_build/test/test_mount_fsx.sh
./devops/docker_build/test/test_mount_s3.sh
./devops/docker_build/test/test_volumes.sh

# AWS.
echo "# Check AWS authentication setup"
if [[ $AWS_ACCESS_KEY_ID == "" ]]; then
    unset AWS_ACCESS_KEY_ID
else
    echo "AWS_ACCESS_KEY_ID='$AWS_ACCESS_KEY_ID'"
fi;

if [[ $AWS_SECRET_ACCESS_KEY == "" ]]; then
    unset AWS_SECRET_ACCESS_KEY
else
    echo "AWS_SECRET_ACCESS_KEY='***'"
fi;

if [[ $AWS_DEFAULT_REGION == "" ]]; then
    unset AWS_DEFAULT_REGION
else
    echo "AWS_DEFAULT_REGION='$AWS_DEFAULT_REGION'"
fi;
aws configure --profile am list

echo "CONTAINER_VERSION='$CONTAINER_VERSION'"
echo "BUILD_TAG='$BUILD_TAG'"

echo "which python: "$(which python)
echo "check pandas package: "$(python -c "import pandas; print(pandas)")

echo "PATH=$PATH"
echo "PYTHONPATH=$PYTHONPATH"
echo "entrypoint.sh: '$@'"

# TODO(gp): eval seems to be more general, but it creates a new executable.
eval "$@"
