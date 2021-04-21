#!/usr/bin/env bash
#
# Execute scripts to initialize the image.
#

set -e
source ~/.bashrc


run_inits () {
    INIT_DIR="devops/docker_build/init"
    if [[ -d "$1/$INIT_DIR" ]];then
        ls -l "$1/$INIT_DIR/"*.sh
        for f in "$1/$INIT_DIR/"*.sh; do
            echo "# Running script: ${f}"
            /bin/bash "$f"
        done
    else
        echo "ERROR: '$1' does not contain '$INIT_DIR' directory"
        exit -1
    fi
}

# Run inits in the current repo.
run_inits "."
