#!/bin/bash -e

# """
# Create a branch, check it out, and push it remotely.
# """

source helpers.sh

execute "git checkout -b $1"

# This doesn't work since we are creating the branch locally and there is no
# remote branch to track.
#execute "git branch --set-upstream-to origin/$1"

# Push the branch remotely.
execute "git commit"
execute "git push -u origin $1"

# To delete a branch just created:
# > git checkout master
# > git branch -d <BRANCH_NAME>
