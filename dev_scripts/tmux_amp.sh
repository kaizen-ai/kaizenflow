#!/bin/bash -e
#
# Create a standard tmux session for the amp repo.
# 
# # Create an amp tmux session for $HOME/src/amp1
# > dev_scripts/tmux_amp.sh amp 1
#
# # Create a cmamp tmux session for $HOME/src/cmamp2
# > dev_scripts/tmux_amp.sh cmamp 2

echo "##> dev_scripts/tmux_amp.sh"

set -x

SERVER_NAME=$(uname -n)
echo "SERVER_NAME=$SERVER_NAME"

if [[ $SERVER_NAME == "gpmac"* ]]; then
  HOME_DIR="/Users/$USER"
else
  # AWS.
  HOME_DIR="/data/$USER"
fi;

# #############################################################################
# Compute IDX.
# #############################################################################

DIR_PREFIX=$1
if [[ -z $DIR_PREFIX ]]; then
  echo "ERROR: you need to specify directory prefix, e.g. 'amp' or 'cmamp'"
fi;

IDX=$2
if [[ -z $IDX ]]; then
  echo "ERROR: You need to specify IDX={1,2,3}"
  exit -1
fi;

AMP_DIR="${HOME_DIR}/src/${DIR_PREFIX}${IDX}"
echo "AMP_DIR=$AMP_DIR"

# #############################################################################
# Prepare the script to run.
# #############################################################################
SETENV="dev_scripts/setenv_amp.sh"

# No `clear` since we want to see issues, if any.
#CMD="source ${SETENV} && reset && clear"
CMD="source ${SETENV}"
TMUX_NAME="${DIR_PREFIX}${IDX}"

# #############################################################################
# Open the tmux windows.
# #############################################################################

tmux new-session -d -s $TMUX_NAME -n "---${TMUX_NAME}---"

# The first one window seems a problem.
tmux send-keys "white; cd ${AMP_DIR} && $CMD" C-m C-m
#
tmux new-window -n "dbash"
tmux send-keys "green; cd ${AMP_DIR} && $CMD" C-m C-m
#
tmux new-window -n "regr"
tmux send-keys "yellow; cd ${AMP_DIR} && $CMD" C-m C-m
#
tmux new-window -n "jupyter"
tmux send-keys "yellow; cd ${AMP_DIR} && $CMD" C-m C-m

# Go to the first tab.
tmux select-window -t $TMUX_NAME:0
tmux -2 attach-session -t $TMUX_NAME
