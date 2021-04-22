#!/usr/bin/env bash

set -e
source ~/.bash_profile

source devops/docker_build/entrypoint/gh_action_aws_credentials.sh
source devops/docker_build/entrypoint/patch_environment_variables.sh

mount -a || true

# Allow working with files outside a container.
umask 000

./devops/docker_build/test/test_mount_fsx.sh
./devops/docker_build/test/test_mount_s3.sh

exec "$@"
