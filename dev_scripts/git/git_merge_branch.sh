#!/bin/bash -xe

# """
# Qualify a branch and then merge it into master.
# """

# TODO(gp): Convert in python.

source helpers.sh

src_branch="$1"
if [[ -z $src_branch ]]; then
    echo "You need to specify a src_branch"
    exit -1
fi;
echo "src_branch=$src_branch"

dst_branch="master"
echo "dst_branch=$dst_branch"

cmd="git fetch origin $src_branch:$src_branch"
execute $cmd

cmd="git fetch origin $dst_branch:$dst_branch"
execute $cmd

cmd="git checkout $src_branch"
execute $cmd

cmd="git_submodules_clean.sh"
execute $cmd
cmd="git_submodules_pull.sh"
execute $cmd

# Align all the submodules markers.
cmd="git_submodules_roll_fwd.sh"
execute $cmd
cmd="git_submodules_are_updated.sh"
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
