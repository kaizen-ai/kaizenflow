#!/usr/bin/env bash

set -xe

# Install conda requirements.

update_env () {
    echo "Installing ${1}"
    conda env update -n ${ENV_NAME} --file ${ENV_FILE}
}

ENV_FILE="devops/docker_build/conda.yml"
update_env ${ENV_FILE}

conda clean --all --yes
