#!/bin/bash -e

# """
# Merge master in the current Git client.
# """

source helpers.sh

#branch="master"
branch="PartTask2335_Re-enable_gluonts"

cmd="git fetch origin $branch:$branch"
execute $cmd

cmd="git pull --autostash"
execute $cmd

cmd="git merge $branch"
execute $cmd

cmd="git push"
execute $cmd
