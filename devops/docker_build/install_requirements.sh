#!/usr/bin/env bash
#
# Install python packages.

set -e
set -x

echo "Installing ${ENV_NAME}"

if [[ 0 == 1 ]]; then
    # Conda flow
    update_env () {
        echo "Installing ${ENV_FILE} in ${ENV_NAME}"
        ENV_FILE=${1}
        conda env update -n ${ENV_NAME} --file ${ENV_FILE}
    }

    AMP_CONDA_FILE="devops/docker_build/conda.yml"
    update_env ${AMP_CONDA_FILE}

    conda clean --all --yes
else
    echo "Building environment with poetry ..."

    # Get the poetry file.
    cp devops/docker_build/pyproject.toml .

    # Print config.
    poetry config --list --local

    # Compute dependencies.
    poetry lock

    if [[ 0 == 1 ]]; then
        # Install with poetry.
        poetry install
    else
        # Install with pip.
        poetry export -f requirements.txt --output requirements.txt

        python -m ${ENV_NAME} ./${ENV_NAME}
        source ${ENV_NAME}/bin/activate
        pip install --upgrade pip
        pip install --no-deps -r requirements.txt
    fi;
fi;
