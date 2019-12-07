#!/bin/bash -e

# """
# Merge master in the current Git client.
# """

source helpers.sh

branch="master"

cmd="git fetch origin $branch:$branch"
execute $cmd

cmd="git pull --autostash"
execute $cmd

cmd="git merge $branch"
execute $cmd

cmd="git push"
execute $cmd
