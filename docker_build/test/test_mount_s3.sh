#!/usr/bin/env bash

MOUNT_POINT="/s3/default00-bucket"
if [ "$(mount | grep -c $MOUNT_POINT)" -lt 1 ]; then
  echo -e """\e[93mWARNING: $MOUNT_POINT not mounted.\e[0m"""
fi
