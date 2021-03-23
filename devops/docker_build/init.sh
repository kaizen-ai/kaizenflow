#!/usr/bin/env bash
#
# Execute scripts to initialize the image.

set -e

source ~/.bashrc
conda activate venv


run_inits () {
    INIT_DIR="devops/docker_build/init"
    if [[ -d "$1/$INIT_DIR" ]];then
        for f in "$1/$INIT_DIR/"*.sh; do
            echo "Running script: ${f}"
            source "$f"
        done
    else
        echo "ERROR: '$1' does not contain '$INIT_DIR' directory"
        exit -1
    fi
}

# Run inits in the current repo.
run_inits "."
