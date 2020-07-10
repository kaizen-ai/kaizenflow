#!/usr/bin/env bash

export PYTHONDONTWRITEBYTECODE=x

export PYTHONPATH="$(pwd):$PYTHONPATH"

MYPYPATH="$(pwd):$MYPYPATH"

export PATH="\
            $(pwd):\
            $(pwd)/dev_scripts:\
            $(pwd)/dev_scripts/aws:\
            $(pwd)/dev_scripts/git:\
            $(pwd)/dev_scripts/infra:\
            $(pwd)/dev_scripts/install:\
            $(pwd)/dev_scripts/notebooks:\
            $(pwd)/dev_scripts/testing:\
            $PATH"


export AMP="."