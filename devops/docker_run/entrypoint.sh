#!/usr/bin/env bash

set -e

FILE_NAME="devops/docker_run/entrypoint.sh"
echo "##> $FILE_NAME"

echo "UID="$(id -u)
echo "GID="$(id -g)

echo "# Activate environment"
source /${ENV_NAME}/bin/activate

source devops/docker_run/setenv.sh

# Allow working with files outside a container.
#umask 000

# Enable dind unless the user specifies otherwise (needed for prod image).
if [ -z "$ENABLE_DIND" ]; then
    ENABLE_DIND=1
    echo "ENABLE_DIND=$ENABLE_DIND"
fi;

if [[ $ENABLE_DIND == 1 ]]; then
    echo "# Setting up Docker-in-docker"
    if [[ ! -d /etc/docker ]]; then
        sudo mkdir /etc/docker
    fi;
    # This is needed to run the database in dind mode (see CmTask309).
    # TODO(gp): For some reason appending to file directly `>>` doesn't work.
    sudo echo '{ "storage-driver": "vfs" }' | sudo tee -a /etc/docker/daemon.json
    # Start Docker Engine.
    sudo /etc/init.d/docker start
    sudo /etc/init.d/docker status
    # Wait for Docker Engine to be started, otherwise `docker.sock` file is 
    # not created so fast. This is needed to change `docker.sock` permissions.
    DOCKER_SOCK_FILE=/var/run/docker.sock
    COUNTER=0
    # Set sleep interval.
    SLEEP_SEC=0.1
    # Which is 10 seconds, i.e. `100 = 10 seconds (limit) / 0.1 seconds (sleep)`.
    COUNTER_LIMIT=100
    while true; do
        if [ -e "$DOCKER_SOCK_FILE" ]; then
            # Change permissions for Docker socket. See more on S/O:
            # `https://stackoverflow.com/questions/48957195`.
            # We do it after the Docker engine is started because `docker.sock` is
            # created only after the engine start.
            # TODO(Grisha): give permissions to the `docker` group only and not to
            # everyone, i.e. `666`.
            sudo chmod 666 $DOCKER_SOCK_FILE
            echo "Permissions for "$DOCKER_SOCK_FILE" have been changed."
            break
        elif [[ "$COUNTER" -gt "$COUNTER_LIMIT" ]]; then
            echo "Timeout limit is reached, exit script."
            exit 1
        else
            COUNTER=$((counter+1))
            sleep $SLEEP_SEC
            echo "Waiting for $DOCKER_SOCK_FILE to be created."
        fi
    done
fi;


# Mount other file systems.
# mount -a || true
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

echo "AM_CONTAINER_VERSION='$AM_CONTAINER_VERSION'"

# Test the installed packages.
if [[ $ENABLE_DIND == 1 ]]; then
    echo "docker -v: "$(docker -v)
    echo "docker-compose -v: "$(docker-compose -v)
fi;
VAL=$(which python)
echo "which python: $VAL"
VAL=$(python -V)
echo "python -V: $VAL"
VAL=$(python -c "import pandas; print(pandas.__version__)")
echo "pandas: $VAL"
VAL=$(python -c "import helpers; print(helpers)")
echo "helpers: $VAL"

echo "PATH=$PATH"
echo "PYTHONPATH=$PYTHONPATH"
echo "entrypoint.sh: '$@'"

# TODO(gp): eval seems to be more general, but it creates a new executable.
eval "$@"
