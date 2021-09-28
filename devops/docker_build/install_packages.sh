#!/usr/bin/env bash
#
# Install packages.
#

set -ex

FILE_NAME="devops/docker_build/install_packages.sh"
echo "##> $FILE_NAME"

DEBIAN_FRONTEND=noninteractive

# Update the package listing, so we know what package exist.
apt-get update

# Install security updates.
apt-get -y upgrade

APT_GET_OPTS="-y --no-install-recommends"

# TODO(gp): We can remove lots of this.
apt-get install $APT_GET_OPTS cifs-utils
apt-get install $APT_GET_OPTS git
apt-get install $APT_GET_OPTS keyutils
apt-get install $APT_GET_OPTS make

# Install vim.
apt-get install $APT_GET_OPTS vim

# This is needed to compile ujson.
# See https://github.com/alphamatic/lm/issues/155
apt-get install $APT_GET_OPTS build-essential autoconf libtool python3-dev

# Install pip.
apt-get install $APT_GET_OPTS python3-venv python3-pip
echo "PYTHON VERSION="$(python3 --version)
echo "PIP VERSION="$(pip3 --version)

# Install poetry.
pip3 install poetry
echo "POETRY VERSION="$(poetry --version)

# Install homebrew.
#apt-get install $APT_GET_OPTS build-essential procps curl file git

# Install github CLI.
# From https://github.com/cli/cli/blob/trunk/docs/install_linux.md
DEBIAN_FRONTEND=noninteractive apt-get install -y tzdata
apt-get install $APT_GET_OPTS software-properties-common
apt-get install $APT_GET_OPTS dirmngr
apt-get install $APT_GET_OPTS gpg-agent
apt-key adv --keyserver keyserver.ubuntu.com --recv-key C99B11DEB97541F0
apt-add-repository https://cli.github.com/packages
apt-get update
apt-get install $APT_GET_OPTS gh
echo "GH VERSION="$(gh --version)

# This is needed to install pygraphviz.
# See https://github.com/alphamatic/amp/issues/1311
# It needs tzdata so it needs to go after installing tzdata.
apt-get install $APT_GET_OPTS libgraphviz-dev

# This is needed to install dot.
apt-get install $APT_GET_OPTS graphviz

# This is needed for Postgres DB.
apt-get postgresql-client $APT_GET_OPTS graphviz

# Clean up.
if [[ $CLEAN_UP_INSTALLATION ]]; then
    echo "Cleaning up installation..."
    apt-get purge -y --auto-remove
    DIRS="/root/.cache /tmp/*"
    du -hs $DIRS | sort -h
    rm -rf $DIRS
else
    echo "No clean up installation"
fi;
