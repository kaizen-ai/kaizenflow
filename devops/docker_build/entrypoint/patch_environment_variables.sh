#!/usr/bin/env bash

set -e

export PYTHONDONTWRITEBYTECODE=x

export PYTHONPATH="$(pwd):$PYTHONPATH"

MYPYPATH="$(pwd):$MYPYPATH"

export PATH="$(pwd):$(pwd)/dev_scripts:$(pwd)/dev_scripts/aws:$(pwd)/dev_scripts/git:$(pwd)/dev_scripts/infra:$(pwd)/dev_scripts/install:$(pwd)/dev_scripts/notebooks:$(pwd)/dev_scripts/testing:$(pwd)/documentation/scripts:$PATH"


# TODO(gp): Is this needed? At least let's call it AMP_DIR.
export AMP="."
