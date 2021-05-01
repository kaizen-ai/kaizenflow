#!/bin/bash -e
#
# Build a thin virtual environment to run workflows on the dev machine.
#

# TODO(gp): Use python 3.9 and keep this in sync with
# devops/docker_build/pyproject.toml

set -e

VENV_DIR="$HOME/src/venv/client_setup"
if [[ -d $VENV_DIR ]]; then
    echo "Deleting old virtual environment in '$VENV_DIR'"
    rm -rf $VENV_DIR
fi;
echo "Creating virtual environment in '$VENV_DIR'"
python -m venv $VENV_DIR
source $VENV_DIR/bin/activate

# Install packages.
python -m pip install --upgrade pip
pip install -r dev_scripts/client_setup/requirements.txt

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

echo "# Activate the virtual env with:"
echo "source $VENV_DIR/bin/activate"
echo "  or"
echo "source dev_scripts/setenv_amp.sh"
