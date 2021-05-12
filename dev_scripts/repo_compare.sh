#!/bin/bash
#
# Compare the dev scripts of different repos.
#

set -x

GIT_CLIENT="lemonade1"
echo "GIT_CLIENT=$GIT_CLIENT"

if [[ 1 == 1 ]]; then
    # amp vs lemonade
    DIR1=$HOME/src/$GIT_CLIENT/amp
    DIR2=$HOME/src/$GIT_CLIENT

    echo "DIR1=$DIR1"
    echo "DIR2=$DIR2"

    vimdiff $DIR1/dev_scripts/go_amp.sh $DIR2/dev_scripts_lem/go_lem.sh
    vimdiff $DIR1/dev_scripts/tmux_amp.sh $DIR2/dev_scripts_lem/tmux_lem.sh
    vimdiff $DIR1/dev_scripts/setenv_amp.sh $DIR2/dev_scripts_lem/setenv.sh
else
    # amp vs dev_tools
    DIR1=$HOME/src/$GIT_CLIENT/amp
    DIR2=$HOME/src/dev_tools1

    echo "DIR1=$DIR1"
    echo "DIR2=$DIR2"

    vimdiff $DIR1/dev_scripts/go_amp.sh $DIR2/dev_scripts/go_dev_tools.sh
    vimdiff $DIR1/dev_scripts/tmux_amp.sh $DIR2/dev_scripts/tmux_dev_tools.sh
    vimdiff $DIR1/dev_scripts/setenv_amp.sh $DIR2/dev_scripts/setenv_dev_tools.sh
fi;
