#!/bin/bash -e

# """
# Create a branch, check it out, and push it remotely.
# """

source helpers.sh

execute "git checkout -b $1"
execute "git branch -u origin/$1"
#execute "git push --set-upstream origin origin $1"
