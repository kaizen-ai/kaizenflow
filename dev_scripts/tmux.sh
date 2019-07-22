#!/bin/bash -e

SERVER_NAME=$(uname -n)
echo "SERVER_NAME=$SERVER_NAME"

if [[ $SERVER_NAME == "gpmac.local" ]]; then
  DIR_NAME1="/Users/saggese/src/utilities"
  DIR_NAME2="/Users/saggese/src/lemonade"
  SETENV="./dev_scripts/setenv.sh"
elif [[ $SERVER_NAME == "ip-172-31-24-5" ]]; then
  DIR_NAME1="/data/saggese/src/utilities"
  DIR_NAME2="/data/saggese/src/lemonade"
  SETENV="./dev_scripts/setenv.sh"
else
  echo "Invalid server"
  exit -1
fi;

TMUX_NAME="dev"

# No clear since we want to see issues.
#CMD="source ${SETENV} && reset && clear"
CMD="source ${SETENV}"

##
tmux new-session -d -s $TMUX_NAME -n "util"
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


tmux new-window -t $TMUX_NAME -n "lemon"
tmux send-keys -t $TMUX_NAME "white; cd ${DIR_NAME2} && $CMD" C-m C-m
#
tmux new-window -t $TMUX_NAME -n " "
tmux send-keys -t $TMUX_NAME "green; cd ${DIR_NAME2} && $CMD" C-m C-m
#
tmux new-window -t $TMUX_NAME -n " "
tmux send-keys -t $TMUX_NAME "yellow; cd ${DIR_NAME2} && $CMD" C-m C-m
#
tmux new-window -t $TMUX_NAME -n "jupy"
tmux send-keys -t $TMUX_NAME "yellow; cd ${DIR_NAME2} && $CMD" C-m C-m

tmux select-window -t $TMUX_NAME:0
tmux -2 attach-session -t $TMUX_NAME
