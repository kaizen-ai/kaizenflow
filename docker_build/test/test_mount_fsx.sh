#!/usr/bin/env bash

MOUNT_POINT="/fsx/research"
if [ "$(mount | grep -c $MOUNT_POINT)" -lt 1 ]; then
  echo -e """
\e[31m$MOUNT_POINT not mounted.\e[0m
Try to update your branch \`git pull origin master\`and restart your docker container."""
fi
