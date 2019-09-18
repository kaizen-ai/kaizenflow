#!/bin/bash -xe

# ```
# - Run linter on the entire tree.
# ```

VERB=DEBUG
ENV_NAME=develop

# Config.
source ~/.bashrc
source dev_scripts/setenv.sh -e $ENV_NAME

# Run (ignoring the rc).
linter.py -d . --num_threads; true
