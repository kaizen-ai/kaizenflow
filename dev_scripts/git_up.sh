#!/bin/bash -e
echo '# Check what are the differences with master...'
cmd="git ll ..origin/master"
echo "> $cmd"
eval $cmd

cmd="git ll origin/master.."
echo "> $cmd"
eval $cmd

echo
echo '# Saving local changes...'
export UUID=$(uuidgen)
export msg="WIP local changes $(date +%Y%m%d-%H%M%S)-$(whoami)-$(hostname) $UUID"
cmd='git stash save --quiet "'$msg'"'
echo "> $cmd"
eval $cmd

echo
echo '# Getting new commits...'
cmd='git pull --rebase'
echo "> $cmd"
eval $cmd

echo
echo '# Restoring local changes...'
cmd="git stash list | head | grep $UUID"
echo "> $cmd"
eval $cmd
cmd='git stash pop --quiet'
echo "> $cmd'"
eval $cmd

echo
echo '# Checking stash status...'
cmd='git stash list'
echo "> $cmd'"
eval $cmd
