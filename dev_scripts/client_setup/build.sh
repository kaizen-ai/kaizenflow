#!/bin/bash -e
#
# Build a thin virtual environment to run workflows on the dev machine.
#

set -e

VENV_DIR="$HOME/venv"
echo "Creating virtual environment in '$VENV_DIR'"
python -m venv $VENV_DIR
source $VENV_DIR/bin/activate

# Install packages.
python -m pip install --upgrade pip
pip install -r dev_scripts/client_setup/requirements.txt

aws --version
invoke --version
gh --version

echo "# Activate the virtual env with:"
echo "source $VENV_DIR/bin/activate"
