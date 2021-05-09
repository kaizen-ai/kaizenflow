#!/bin/bash -e

SERVER_NAME=$(uname -n)
echo "SERVER_NAME=$SERVER_NAME"

if [[ $SERVER_NAME == "gpmac"* ]]; then
  HOME_DIR="/Users/saggese"
elif [[ $SERVER_NAME == "ip-*" ]]; then
  # AWS.
  HOME_DIR="/data/saggese"
else
  echo "Invalid server '$SERVER_NAME'"
  exit -1
fi;

# #############################################################################
# Compute TARGET.
# #############################################################################
AMP_DIR="$HOME_DIR/src/amp"
echo "AMP_DIR=$AMP_DIR"

# #############################################################################
# Prepare the script to run.
# #############################################################################
SETENV="./dev_scripts/setenv_amp.sh"
# No clear since we want to see issues.
#CMD="source ${SETENV} && reset && clear"
CMD="source ${SETENV}"
TMUX_NAME="amp1"

# #############################################################################
# Open the tmux windows.
# #############################################################################

tmux new-session -d -s $TMUX_NAME -n "---AMP---"

# The first one window seems a problem.
tmux send-keys -t $TMUX_NAME "white; cd ${AMP_DIR} && $CMD" C-m C-m

#
tmux new-window -t $TMUX_NAME -n "dbash"
tmux send-keys -t $TMUX_NAME "green; cd ${AMP_DIR} && $CMD" C-m C-m
#
tmux new-window -t $TMUX_NAME -n " "
tmux send-keys -t $TMUX_NAME "yellow; cd ${AMP_DIR} && $CMD" C-m C-m
#
tmux new-window -t $TMUX_NAME -n " "
tmux send-keys -t $TMUX_NAME "yellow; cd ${AMP_DIR} && $CMD" C-m C-m

# Go to the first tab.
tmux select-window -t $TMUX_NAME:0
tmux -2 attach-session -t $TMUX_NAME
