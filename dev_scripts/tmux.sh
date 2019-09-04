#!/bin/bash -e

SERVER_NAME=$(uname -n)
echo "SERVER_NAME=$SERVER_NAME"

if [[ $SERVER_NAME == "gpmac.local" || $SERVER_NAME == "gpmac.lan" ]]; then
  HOME_DIR="/Users/saggese"
elif [[ $SERVER_NAME == "ip-*" ]]; then
  HOME_DIR="/data/saggese"
else
  echo "Invalid server"
  exit -1
fi;

DIR_NAME1="$HOME_DIR/src/amp"

SETENV="./dev_scripts/setenv.sh"

TMUX_NAME="dev"

# No clear since we want to see issues.
#CMD="source ${SETENV} && reset && clear"
CMD="source ${SETENV}"

##
tmux new-session -d -s $TMUX_NAME -n "amp"
# The first one window seems a problem.
tmux send-keys -t $TMUX_NAME "white; cd ${DIR_NAME1} && $CMD" C-m C-m

#
tmux new-window -t $TMUX_NAME -n " "
tmux send-keys -t $TMUX_NAME "green; cd ${DIR_NAME1} && $CMD" C-m C-m
#
tmux new-window -t $TMUX_NAME -n " "
tmux send-keys -t $TMUX_NAME "yellow; cd ${DIR_NAME1} && $CMD" C-m C-m
#
tmux new-window -t $TMUX_NAME -n " "
tmux send-keys -t $TMUX_NAME "yellow; cd ${DIR_NAME1} && $CMD" C-m C-m


tmux select-window -t $TMUX_NAME:0
tmux -2 attach-session -t $TMUX_NAME
