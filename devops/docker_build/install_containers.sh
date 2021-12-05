#!/usr/bin/env bash

set -ex

whoami
uname -a
echo $HOME
ls -l

FILE_NAME="devops/docker_build/install_containers.sh"
echo "#############################################################################"
echo "##> $FILE_NAME"
echo "#############################################################################"

# This file is copied from the Docker installation inside the container from
# `/etc/init.d/docker`.
cp etc_init_d_docker /etc/init.d/docker

/etc/init.d/docker start
/etc/init.d/docker status
sudo docker pull postgres:13
