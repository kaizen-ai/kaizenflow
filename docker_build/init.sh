#!/bin/bash

source ~/.bashrc
conda activate venv

run_inits (){
    if [[ -d "$1/docker_build/init" ]];then
        echo "++++++ $1 contain directory"
        for f in "$1/docker_build/init/"*.sh; do
            echo "Running script: ${f}"
            source "$f"
        done
    else
        echo "------ $1 no contain directory"
    fi
}

# Run inits in the current repo
run_inits "."
