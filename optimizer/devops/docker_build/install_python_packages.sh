#!/usr/bin/env bash
#
# Install Python packages.
#

set -ex

FILE_NAME="devops/docker_build/install_python_packages.sh"
echo "#############################################################################"
echo "##> $FILE_NAME"
echo "#############################################################################"

echo "# Installing ${ENV_NAME}"


# Install with poetry inside a venv.
echo "# Install with venv + poetry"

python3 -m ${ENV_NAME} /${ENV_NAME}
source /${ENV_NAME}/bin/activate

pip3 install wheel

poetry install

poetry env list


# Some tools refer to `python` and `pip`.
# TODO(gp): Move to install_packages.sh
if [[ ! -e /usr/bin/python ]]; then
    ln -s /usr/bin/python3 /usr/bin/python
fi;
if [[ ! -e /usr/bin/pip ]]; then
    ln -s /usr/bin/pip3 /usr/bin/pip
fi;