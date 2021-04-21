#!/usr/bin/env bash

# TODO(gp): Move all the PATH and PYTHONPATH to the entrypoint.

set -e

devops/docker_build/entrypoint/aws_credentials.sh

source ~/.bash_profile

source devops/docker_build/entrypoint/patch_environment_variables.sh

mount -a || true

# Allow working with files outside a container.
umask 000

./devops/docker_build/test/test_mount_fsx.sh
./devops/docker_build/test/test_mount_s3.sh
./devops/docker_build/test/test_volumes.sh

#echo "PATH=$PATH"
#echo "PYTHONPATH=$PYTHONPATH"
#echo "entrypoint.sh: '$@'"
# TODO(gp): eval seems to be more general, but it creates a new executable.
eval "$@"
