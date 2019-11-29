#!/bin/bash -e

# """
# Script to roll fwd the submodules.
# """

# TODO(gp): Ensure that all submodules are clean.

AMP_DIR="amp"
BRANCH="master"
echo "BRANCH=$BRANCH"

source $AMP_DIR/dev_scripts/helpers.sh

echo "# amp pointer in p1 is at:"
cmd="git ls-tree master | grep amp"
execute $cmd

cmd="git submodule update --remote"
execute $cmd

# TODO(gp): Factor out this code in a loop.

# Pull p1
echo "+ Pull p1"
git checkout $BRANCH
git pull --autostash
#git clean -fd

# Pull amp
echo "+ Pull amp"
cd amp

cmd="dev_scripts/git/git_hash_head.sh"
execute $cmd

git checkout $BRANCH
git pull --autostash
#git clean -fd

cmd="dev_scripts/git/git_hash_head.sh"
execute $cmd

cd ..
git add amp

# Pull infra
echo "+ Pull infra"
cd infra
git checkout $BRANCH
git pull --autostash
#git clean -fd
cd ..
git add infra

echo "# amp pointer in p1 is at:"
cmd="git ls-tree master | grep amp"
execute $cmd

cmd='git commit -am "Move fwd amp and infra" && git push'
echo "Run:"
echo "> $cmd"
