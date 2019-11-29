#!/bin/bash -xe

# TODO(gp): Convert in python.

branch="$1"
if [[ -z $branch ]]; then
    echo "You need to specify a branch"
    exit -1
fi;

# Merge master into branch.
git checkout master
git pull

# Align all the submodules markers.
dev_scripts/git_roll_fwd_submodules.sh
dev_scripts/git_are_submodules_updated.sh

git checkout $branch
git merge master

# Lint.
linter.py -f $(git diff --name-only master...)

# Run tests.
pytest
