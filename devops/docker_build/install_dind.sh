#!/usr/bin/env bash
#
# Install Docker-in-Docker.
#

# From https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-18-04

set -ex

FILE_NAME="devops/docker_build/install_dind.sh"
echo "#############################################################################"
echo "##> $FILE_NAME"
echo "#############################################################################"

# Install Docker.
apt install -y apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu bionic stable"
apt update
apt-cache policy docker-ce
apt install -y docker-ce

# Install the latest version of `docker-compose`.
# `https://docs.docker.com/compose/install/`.
COMPOSE_LATEST_VERSION=$(curl https://api.github.com/repos/docker/compose/releases/latest | jq .name -r)
COMPOSE_DIR="/usr/bin/docker-compose"
curl -L https://github.com/docker/compose/releases/download/$COMPOSE_LATEST_VERSION/docker-compose-$(uname -s)-$(uname -m) -o $COMPOSE_DIR
# Apply executable permissions to the binary.
chmod +x $COMPOSE_DIR
# Check version.
COMPOSE_VERSION=$(docker-compose --version)
echo "docker-compose-version=$COMPOSE_VERSION"
