#!/usr/bin/env bash

set -e

FILE_NAME="devops/docker_run/entrypoint.sh"
echo "##> $FILE_NAME"

echo "UID="$(id -u)
echo "GID="$(id -g)

echo "# Activate environment"
source /${ENV_NAME}/bin/activate

source devops/docker_run/setenv.sh

#mount -a || true

# Allow working with files outside a container.
#umask 000

if [[ 0 == 1 ]]; then
    # Needed to run database (see CmTask309).
    sudo mkdir /etc/docker
    sudo echo '{ "storage-driver": "vfs" }' | sudo tee -a /etc/docker/daemon.json

    # Start Docker Engine.
    sudo /etc/init.d/docker start
    sudo /etc/init.d/docker status

    # sudo change perms to /mnt/tmpfs

    # Check set-up.
    ./devops/docker_run/test_setup.sh
fi;

if [[ 1 == 0 ]]; then
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
fi;

echo "CONTAINER_VERSION='$CONTAINER_VERSION'"
echo "BUILD_TAG='$BUILD_TAG'"

# TODO(gp): Use this idiom everywhere (see CmampTask387).
VAL=$(which python)
echo "which python: $VAL"
VAL=$(python -V)
echo "python -V: $VAL"
#echo "check pandas package: "$(python -c "import pandas; print(pandas)")
#echo "docker -v: "$(docker -v)
#echo "docker-compose -v: "$(docker-compose -v)
VAL=$(python -c "import helpers; print(helpers)")
echo "helpers: $VAL"
VAL=$(python -c "import cvxpy; print(cvxpy.__version__)")
echo "cvxpy -V: $VAL"

echo "PATH=$PATH"
echo "PYTHONPATH=$PYTHONPATH"
echo "entrypoint.sh: '$@'"

# TODO(gp): eval seems to be more general, but it creates a new executable.
eval "$@"
