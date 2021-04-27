#!/usr/bin/env bash

# TODO(gp): This is not needed since we are centralizing in the scripts called
# by entrypoint.sh.

set -e

echo $AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY > /etc/passwd-s3fs-default00-bucket
chmod 600 /etc/passwd-s3fs-default00-bucket
