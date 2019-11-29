#!/bin/bash -e

# """
# Script to force a git pull of all the repos.
# """

AMP_DIR="amp"

source $AMP_DIR/dev_scripts/helpers.sh

branch="master"
echo "branch=$branch"

cmd="git submodule update --remote"
execute $cmd

cmd="git submodule foreach git pull --autostash"
execute $cmd
