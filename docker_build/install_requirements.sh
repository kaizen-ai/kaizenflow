#!/bin/bash

# TODO(Sergey): Why not using `#!/usr/bin/env bash` here?

# Install conda requirements.

update_env () {
    echo "Installing ${1}"
    cp ${APP_DIR}/$1 "${ENV_DIR}/requirements_dev.yaml"
    conda env update -n ${ENV_NAME} --file "${ENV_DIR}/requirements_dev.yaml"
}

AMP_ENV_FILE="dev_scripts/install/requirements/amp_develop.yaml"
update_env ${AMP_ENV_FILE}

conda clean --all --yes
