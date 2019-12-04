#!/bin/bash -xe

# """
# Qualify a branch with multiple Git repos and then merge it into master.
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

cmd="git diff --name-only $dst_branch..."
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

# Lint super-module.
FILES=$(git diff --name-only $dst_branch...)
cmd="linter.py -f $FILES"
execute $cmd

# Lint amp.
AMP_FILES=$(cd amp && git diff --name-only $dst_branch...)
cmd="(cd amp && linter.py -f $AMP_FILES)"
execute $cmd

# Run tests.
cmd='run_tests.py --num_cpus -1 && (cd amp; run_tests.py --num_cpus -1)'
execute $cmd

# TODO(gp): If everything passes `git push`.
# TODO(gp): Delete branch.
