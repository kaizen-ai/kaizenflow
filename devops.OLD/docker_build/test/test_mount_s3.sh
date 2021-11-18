#!/usr/bin/env bash

# TODO(gp): Merge into test_volumes.sh

set -e

MOUNT_POINT="/s3/default00-bucket"
if [ "$(mount | grep -c $MOUNT_POINT)" -lt 1 ]; then
  echo -e """\e[33mWARNING\e[0m: $MOUNT_POINT not mounted."""
fi
