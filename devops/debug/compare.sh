#!/bin/bash
DIR1=$HOME/src/commodity_research1
DIR2=$HOME/src/commodity_research1/amp

#DIR1=$HOME/src/commodity_research1/amp
#DIR2=$HOME/src/dev_tools

echo "DIR1=$DIR1"
echo "DIR2=$DIR2"

vimdiff {$DIR1,$DIR2}/Makefile
vimdiff {$DIR1,$DIR2}/devops/makefiles/general.mk
vimdiff {$DIR1,$DIR2}/devops/makefiles/repo_specific.mk

diff_to_vimdiff.py --dir1 $DIR1/devops --dir2 $DIR2/devops -o tmp.sh
# --only_different_files
