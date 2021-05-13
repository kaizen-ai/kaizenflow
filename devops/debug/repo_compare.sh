#!/bin/bash
#
# Compare the build system of different repos.
#

GIT_CLIENT="lemonade1"
echo "GIT_CLIENT=$GIT_CLIENT"

if [[ 0 == 1 ]]; then
    # amp vs lemonade
    DIR1=$HOME/src/$GIT_CLIENT/amp
    DIR2=$HOME/src/$GIT_CLIENT
else
    # amp vs dev_tools
    DIR1=$HOME/src/$GIT_CLIENT/amp
    DIR2=$HOME/src/dev_tools1
fi;

echo "DIR1=$DIR1"
echo "DIR2=$DIR2"

if [[ 0 == 1 ]]; then
    # The file storing configuration that should be kept in sync are:
    for file in .dockerignore .gitignore .pre-commit-config.yaml conftest.py dev_scripts/setenv_dev_tools.sh invoke.yaml lib_tasks.py mypy.ini pre-commit.sh pytest.ini tasks.py
    do
        vimdiff {$DIR1,$DIR2}/$file
    done;

    exit -1
fi;

if [[ 1 == 1 ]]; then
    DIR="devops"
    echo "Comparing $DIR"
    diff_to_vimdiff.py --dir1 $DIR1/$DIR --dir2 $DIR2/$DIR
    # --only_different_files
else
    DIR=".github/workflows"
    echo "Comparing $DIR"
    diff_to_vimdiff.py --dir1 $DIR1/$DIR --dir2 $DIR2/$DIR
fi;

#dev_scripts/client_setup/

# TODO(gp): If a file is a link, do not compare.
