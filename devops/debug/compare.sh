#!/bin/bash

if [[ 1 == 1 ]]; then
    # amp vs p1
    DIR1=$HOME/src/...1/amp
    DIR2=$HOME/src/...1
else
    # amp vs dev_tools
    DIR1=$HOME/src/...1/amp
    DIR2=$HOME/src/dev_tools
fi;

echo "DIR1=$DIR1"
echo "DIR2=$DIR2"

if [[ 1 == 1 ]]; then
    vimdiff {$DIR1,$DIR2}/Makefile
    vimdiff {$DIR1,$DIR2}/devops/makefiles/general.mk
    vimdiff {$DIR1,$DIR2}/devops/makefiles/repo_specific.mk

    diff_to_vimdiff.py --dir1 $DIR1/devops --dir2 $DIR2/devops -o tmp.sh
    # --only_different_files
else
    DIR=".github/workflows"
    diff_to_vimdiff.py --dir1 $DIR1/$DIR --dir2 $DIR2/$DIR -o tmp.sh
fi;
