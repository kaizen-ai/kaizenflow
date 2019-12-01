#!/bin/bash -xe

# """
# Qualify a branch and then merge it.
# """

# TODO(gp): Convert in python.

source helpers.sh

branch="$1"
if [[ -z $branch ]]; then
    echo "You need to specify a branch"
    exit -1
fi;
echo "branch=$branch"

cmd="git fetch origin $branch:$branch"
execute $cmd

cmd="git_submodules_clean.sh"
execute $cmd
cmd="git_submodules_pull.sh"
execute $cmd

# Align all the submodules markers.
cmd="git_roll_fwd_submodules.sh"
execute $cmd
cmd="git_are_submodules_updated.sh"
execute $cmd

# Lint p1.
cmd='linter.py -f $(git diff --name-only master...)'
execute $cmd

# Lint amp.
cmd='(cd amp && linter.py -f $(git diff --name-only master...))'
execute $cmd

# Run tests.
#run_tests.py --all
cmd='run_tests.py --num_cpus -1 && (cd amp; run_tests.py --num_cpus -1)'
execute $cmd

# TODO(gp): If everything passes `git push`.
# TODO(gp): Delete branch.
