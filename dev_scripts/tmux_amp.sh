#!/bin/bash -e

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
IDX=$1
if [[ -z $IDX ]]; then
  echo "ERROR: You need to specify IDX={1,2,3}"
  exit -1
fi;

AMP_DIR="$HOME_DIR/src/amp$IDX"
echo "AMP_DIR=$AMP_DIR"

# #############################################################################
# Prepare the script to run.
# #############################################################################
SETENV="dev_scripts/setenv_amp.sh"

# No `clear` since we want to see issues, if any.
#CMD="source ${SETENV} && reset && clear"
CMD="source ${SETENV}"
TMUX_NAME="amp$IDX"

# #############################################################################
# Open the tmux windows.
# #############################################################################

tmux new-session -d -s $TMUX_NAME -n "---AMP$IDX---"

# The first one window seems a problem.
tmux send-keys "white; cd ${AMP_DIR} && $CMD" C-m C-m
#
tmux new-window -n "dbash"
tmux send-keys "green; cd ${AMP_DIR} && $CMD" C-m C-m
#
tmux new-window -n " "
tmux send-keys "yellow; cd ${AMP_DIR} && $CMD" C-m C-m
#
tmux new-window -n " "
tmux send-keys "yellow; cd ${AMP_DIR} && $CMD" C-m C-m

# Go to the first tab.
tmux select-window -t $TMUX_NAME:0
tmux -2 attach-session -t $TMUX_NAME
