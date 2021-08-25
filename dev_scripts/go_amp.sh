#!/bin/bash -e

# set -x

IDX=$1
if [[ -z $IDX ]]; then
    echo "ERROR: You need to specify a client, like 1, 2, 3..."
    exit -1
fi;

DIR_NAME="$HOME/src/cmamp$IDX"
FILE="dev_scripts/tmux_amp.sh $IDX"
echo "> $DIR/$FILE"

cd $DIR_NAME
exec $FILE
