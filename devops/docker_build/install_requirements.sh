#!/usr/bin/env bash
#
# Install python packages.
#

set -e

echo "# Installing ${ENV_NAME}"

if [[ 0 == 1 ]]; then
    # Conda flow
    echo "# Building environment with conda"
    update_env () {
        echo "Installing ${ENV_FILE} in ${ENV_NAME}"
        ENV_FILE=${1}
        conda env update -n ${ENV_NAME} --file ${ENV_FILE}
    }

    AMP_CONDA_FILE="devops/docker_build/conda.yml"
    update_env ${AMP_CONDA_FILE}

    conda clean --all --yes
else
    echo "# Building environment with poetry"

    # Print config.
    poetry config --list --local

    # Compute dependencies.
    poetry lock

    if [[ 0 == 1 ]]; then
        # Install with poetry.
        echo "# Install with poetry"
        poetry install

        # poetry prepends a `.` to the env.
        ln -sf .${ENV_NAME} ${ENV_NAME}
    else
        # Install with pip.
        echo "# Install with venv + poetry"
        #poetry export -f requirements.txt --output requirements.txt

        python3 -m ${ENV_NAME} /${ENV_NAME}
        source /${ENV_NAME}/bin/activate
        #pip3 install --upgrade pip
        #pip3 install --no-deps -r requirements.txt
        poetry install

        # TODO(gp): Clean up.
    fi;

    poetry env list

    # Clean up.
    # TODO(gp): Enable this.
    #poetry cache clear --all -q pypi
fi;

# Some tools refer to `python` and `pip`.
ln -s /usr/bin/python3 /usr/bin/python
ln -s /usr/bin/pip3 /usr/bin/pip
