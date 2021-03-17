#!/usr/bin/env bash
#
# Install conda requirements.

set -e

update_env () {
    ENV_FILE=${1}
    echo "Installing $ENV_FILE in ${ENV_NAME}"
    conda env update -n ${ENV_NAME} --file ${ENV_FILE}
}

AMP_CONDA_FILE="devops/docker_build/conda.yml"
update_env ${AMP_CONDA_FILE}

conda clean --all --yes
