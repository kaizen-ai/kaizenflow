#!/usr/bin/env bash
#
# Install conda requirements.

set -e

update_env () {
    ENV_FILE=${1}
    echo "Installing ${1}"
    conda env update -n ${ENV_NAME} --file ${AMP_CONDA_FILE}
}

AMP_CONDA_FILE="devops/docker_build/conda.yml"
update_env ${AMP_CONDA_FILE}

conda clean --all --yes
