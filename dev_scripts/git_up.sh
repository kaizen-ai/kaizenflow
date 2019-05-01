#!/bin/bash -e
echo '[stash] saving local changes...'
export UUID=$(uuidgen)
cmd='git stash save --quiet "WIP local changes $(date +%Y%m%d%H%M%S)-$(whoami)-$(hostname) $UUID"'
echo "> $cmd"
eval $cmd

echo '[pull ] getting new commits...'
cmd='git pull --rebase'
echo "> $cmd"
eval $cmd
cmd='git stash list | head | grep $UUID'
echo "> $cmd"
eval $cmd

echo '[stash] restoring local changes...'
cmd='git stash pop --quiet'
echo "> $cmd'"
eval $cmd
