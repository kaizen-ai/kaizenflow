#!/bin/bash -e
#
# Build a thin virtual environment to run workflows on the dev machine.
#

# TODO(gp): Use python 3.9 and keep this in sync with
# devops/docker_build/pyproject.toml

set -e

# TODO(gp): -> amp.client_venv or amp.venv
VENV_DIR="$HOME/src/venv/client_setup"
if [[ -d $VENV_DIR ]]; then
    echo "Deleting old virtual environment in '$VENV_DIR'"
    rm -rf $VENV_DIR
fi;
echo "Creating virtual environment in '$VENV_DIR'"
python3 -m venv $VENV_DIR
source $VENV_DIR/bin/activate

# Install packages.
# TODO(gp): Switch to poetry.
python3 -m pip install --upgrade pip
pip3 install -r dev_scripts/client_setup/requirements.txt

echo "aws version="$(aws --version)
echo "invoke version="$(invoke --version)
echo "poetry version="$(poetry --version)

# Update brew.
brew update
echo "brew version="$(brew --version)

# Install GitHub CLI.
brew install gh
echo "gh version="$(gh --version)

# Install dive.
# https://github.com/wagoodman/dive
#brew install dive
#echo "dive version="$(dive --version)

echo "# Configure your client with:"
echo "> source dev_scripts/setenv_amp.sh"
