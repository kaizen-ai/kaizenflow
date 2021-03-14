#!/usr/bin/env bash

set -e

MOUNT_POINT="/fsx/research"
if [ "$(mount | grep -c $MOUNT_POINT)" -lt 1 ]; then
  echo -e """\e[93mWARNING: $MOUNT_POINT not mounted.\e[0m"""
fi
