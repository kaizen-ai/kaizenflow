#!/usr/bin/env bash

# This script is very chatty so we don't echo the commands.
set -e

# Execute all the scripts under the init dir `docker_build/init`.

source ~/.bashrc
conda activate venv

INIT_DIR="docker_build/init"

run_inits (){
    if [[ -d "$1/$INIT_DIR" ]];then
        echo "++++++ $1 contains $INIT_DIR directory"
        for f in "$1/$INIT_DIR/"*.sh; do
            echo "Running script: ${f}"
            source "$f"
        done
    else
        echo "------ $1 does not contain $INIT_DIR directory"
    fi
}

# Run inits in the current repo.
run_inits "."
