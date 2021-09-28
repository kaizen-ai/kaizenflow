#!/usr/bin/env bash
#
# Install python packages.
#

set -ex

FILE_NAME="devops/install_requirements.sh"
echo "##> $FILE_NAME"

echo "# Installing ${ENV_NAME}"

# Install with 'poetry' inside a 'venv'.
echo "# Install with venv + poetry"

# Activate the virtual environment.
python3 -m ${ENV_NAME} /${ENV_NAME}
source /${ENV_NAME}/bin/activate

# Install 'wheel'.
pip3 install wheel

# Install the Python packages with 'poetry'
poetry install

# List the virtual environments.
poetry env list

