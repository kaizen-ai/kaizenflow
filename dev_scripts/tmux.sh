#!/bin/sh -e

#DIR_NAME="/Users/gp/src/git_projects/time_series_analysis"
if [[ $(uname -n) == "gpmac.lan" ]]; then
  # My laptop.
  DIR_NAME="/Users/saggese/src/git_gp1"
  SETENV="./dev_scripts/setenv.sh"
else
  DIR_NAME="/Users/gp/src/git_gp1"
  SETENV="./dev_scripts/setenv2.sh"
fi;

TMUX_NAME="git_gp1"

CMD="source ${SETENV} && clear"

# For debug to see what's happening.
#CMD="source ${SETENV}"

##
tmux new-session -d -s $TMUX_NAME -n "SRC1"

# The first one window seems a problem.
tmux new-window -t $TMUX_NAME -n " "
tmux send-keys -t $TMUX_NAME "white; cd ${DIR_NAME} && $CMD" C-m C-m
#
tmux new-window -t $TMUX_NAME -n " "
tmux send-keys -t $TMUX_NAME "green; cd ${DIR_NAME} && $CMD" C-m C-m
#
tmux new-window -t $TMUX_NAME -n " "
tmux send-keys -t $TMUX_NAME "yellow; cd ${DIR_NAME} && $CMD" C-m C-m


tmux new-window -t $TMUX_NAME -n "latex"
tmux send-keys -t $TMUX_NAME "green; cd ${DIR_NAME} && $CMD && cd memory_builder" C-m C-m
#

tmux select-window -t $TMUX_NAME:0
tmux -2 attach-session -t $TMUX_NAME
