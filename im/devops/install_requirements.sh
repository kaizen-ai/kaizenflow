#!/usr/bin/env bash
#
# Install python packages.
#

set -ex

FILE_NAME="devops/docker_build/install_requirements.sh"
echo "##> $FILE_NAME"

echo "# Installing ${ENV_NAME}"

# Install with poetry inside a venv.
echo "# Install with venv + poetry"

python3 -m ${ENV_NAME} /${ENV_NAME}
source /${ENV_NAME}/bin/activate

pip3 install wheel

poetry install

poetry env list

