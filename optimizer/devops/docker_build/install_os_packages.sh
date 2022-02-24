#!/usr/bin/env bash
#
# Install OS level packages.
#

set -ex

FILE_NAME="devops/docker_build/install_os_packages.sh"
echo "#############################################################################"
echo "##> $FILE_NAME"
echo "#############################################################################"

DEBIAN_FRONTEND=noninteractive

# Update the package listing, so we know what package exist.
apt-get update

# Install security updates.
apt-get -y upgrade

APT_GET_OPTS="-y --no-install-recommends"

# Install pip.
apt-get install $APT_GET_OPTS python3-venv python3-pip
echo "PYTHON VERSION="$(python3 --version)
echo "PIP VERSION="$(pip3 --version)

# Install poetry.
pip3 install poetry
echo "POETRY VERSION="$(poetry --version)

# Install sudo.
apt-get install $APT_GET_OPTS sudo