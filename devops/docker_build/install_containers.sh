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

#ulimit -n 65536

/etc/init.d/docker start
/etc/init.d/docker status
#sudo docker pull postgres:13
