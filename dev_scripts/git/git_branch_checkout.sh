#!/bin/bash -e

# """
# Create a branch, check it out, and push it remotely.
# """

source helpers.sh

# TODO(gp): Make sure you are on master.

cmd="git pull --autostash"
execute $cmd

cmd="git checkout -b $1"
execute $cmd

# This doesn't work since we are creating the branch locally and there is no
# remote branch to track.
#execute "git branch --set-upstream-to origin/$1"

# Push the branch remotely.
cmd="git commit"
execute $cmd
cmd="git push -u origin $1"
execute $cmd

# To delete a branch just created:
# > git checkout master
# > git branch -d <BRANCH_NAME>
