#!/usr/bin/env bash

set -e

export PYTHONDONTWRITEBYTECODE=x

export PYTHONPATH="$(pwd):$PYTHONPATH"

# mypy path.
MYPYPATH="$(pwd):$MYPYPATH"

# Update path.
export PATH="$(pwd):$PATH"
export PATH="$(pwd)/dev_scripts:$PATH"
export PATH="$(pwd)/dev_scripts/aws:$PATH"
export PATH="$(pwd)/dev_scripts/git:$PATH"
export PATH="$(pwd)/dev_scripts/infra:$PATH"
export PATH="$(pwd)/dev_scripts/install:$PATH"
export PATH="$(pwd)/dev_scripts/notebooks:$PATH"
export PATH="$(pwd)/dev_scripts/testing:$PATH"
export PATH="$(pwd)/documentation/scripts:$PATH"


# TODO(gp): Is this needed? At least let's call it AMP_DIR.
export AMP="."
