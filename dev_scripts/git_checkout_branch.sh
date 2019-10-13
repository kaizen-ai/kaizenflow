#!/bin/bash -e

# """
# Create a branch, check it out, and push it remotely.
# """

source helpers.sh

execute "git checkout -b $*"
execute "git push --set-upstream origin origin $*"
