#!/bin/bash -e

# """
# Script to roll fwd the submodules.
# """

# TODO(gp): Ensure that all submodules are clean.

AMP_DIR="amp"

source $AMP_DIR/dev_scripts/helpers.sh

echo "# amp pointer in p1 is at:"
cmd="git ls-tree master | grep amp"
execute $cmd

cmd="git submodule update --remote"
execute $cmd

# TODO(gp): Factor out this code in a loop.

branch="master"
echo "branch=$branch"

# Pull p1
echo "+ Pull p1"
git checkout $branch
git pull
git clean -fd

# Pull amp
echo "+ Pull amp"
cd amp

cmd="dev_scripts/git_hash_head.sh"
execute $cmd

git checkout $branch
git pull
git clean -fd

cmd="dev_scripts/git_hash_head.sh"
execute $cmd

cd ..
git add amp

# Pull infra
echo "+ Pull infra"
cd infra
git checkout $branch
git pull
git clean -fd
cd ..
git add infra

echo "# amp pointer in p1 is at:"
cmd="git ls-tree master | grep amp"
execute $cmd

cmd='git commit -am "Move fwd amp and infra" && git push'
echo "Run:"
echo "> $cmd"
