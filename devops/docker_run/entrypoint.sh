#!/usr/bin/env bash

set -e

FILE_NAME="devops/docker_run/entrypoint.sh"
echo "##> $FILE_NAME"

echo "UID="$(id -u)
echo "GID="$(id -g)

echo "# Activate environment"
source /${ENV_NAME}/bin/activate

source devops/docker_run/setenv.sh

echo "Testing sudo"

#mount -a || true

# Allow working with files outside a container.
#umask 000

echo "Setting up Docker"
if [[ ! -d /etc/docker ]]; then
    sudo mkdir /etc/docker
fi;
# This is needed to run the database in dind mode (see CmTask309).
# TODO(gp): For some reason appending to file directly `>>` doesn't work.
sudo echo '{ "storage-driver": "vfs" }' | sudo tee -a /etc/docker/daemon.json

# Start Docker Engine.
sudo /etc/init.d/docker start
sudo /etc/init.d/docker status

# sudo change perms to /mnt/tmpfs

# Check set-up.
./devops/docker_run/test_setup.sh

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
aws configure --profile am list || true

echo "CONTAINER_VERSION='$CONTAINER_VERSION'"
echo "BUILD_TAG='$BUILD_TAG'"

echo "which python: "$(which python)
echo "python -V: "$(python -V)
#echo "check pandas package: "$(python -c "import pandas; print(pandas)")
echo "docker -v: "$(docker -v)
echo "docker-compose -v: "$(docker-compose -v)

echo "PATH=$PATH"
echo "PYTHONPATH=$PYTHONPATH"
echo "entrypoint.sh: '$@'"

# TODO(gp): eval seems to be more general, but it creates a new executable.
eval "$@"
