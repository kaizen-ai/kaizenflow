#!/usr/bin/env bash
#
# Install python packages.
#

set -e
set -x

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

    # Get the poetry files.
    #cp devops/docker_build/pyproject.toml .
    #cp devops/docker_build/poetry.lock .

    # Print config.
    poetry config --list --local

    # Compute dependencies.
    poetry lock

    if [[ 1 == 1 ]]; then
        # Install with poetry.
        echo "# Install with poetry"
        poetry install

        # poetry prepends a `.` to the env.
        ln -sf .${ENV_NAME} ${ENV_NAME}

        # Clean up.
        # TODO(gp): Enable this.
        #poetry cache clear --all -q pypi
    else
        # Install with pip.
        echo "# Install with pip"
        poetry export -f requirements.txt --output requirements.txt

        python3 -m ${ENV_NAME} ./${ENV_NAME}
        source ${ENV_NAME}/bin/activate
        pip3 install --upgrade pip
        pip3 install --no-deps -r requirements.txt
    fi;
fi;

# Configure bashrc.
BASH_INIT="~/.bash_profile"
touch $BASH_INIT
echo "source ${ENV_NAME}/bin/activate" >>$BASH_INIT
echo "set -o vi" >>$BASH_INIT

# Some tools refer to `python` and `pip`.
ln -s /usr/bin/python3 /usr/bin/python
ln -s /usr/bin/pip3 /usr/bin/pip
