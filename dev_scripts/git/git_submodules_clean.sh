#!/bin/bash -e

# """
# Script to clean all the submodules.
# """

AMP_DIR="amp"

source $AMP_DIR/dev_scripts/helpers.sh

cmd="git clean -fd"
execute $cmd

cd amp
cmd="git clean -fd"
execute $cmd
cd ..

cd infra
cmd="git clean -fd"
execute $cmd
cd ..
