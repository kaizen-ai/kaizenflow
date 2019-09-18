#!/bin/bash -xe

# ```
# Build conda env from scratch
# ```

source ~/.bashrc

export PYTHONPATH=""

# Test conda.
conda activate base

# Configure environment.
#source dev_scripts/setenv.sh
source dev_scripts/setenv.sh -e base

env

create_conda.py --env_name develop --req_file dev_scripts/install/requirements/develop.txt --delete_env_if_exists
