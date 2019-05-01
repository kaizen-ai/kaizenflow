#!/bin/bash -e

source dev_scripts/setenv.sh

OS_NAME=$(uname)
echo "OS_NAME='$OS_NAME'"
SERVER_NAME=$(uname -n)
echo "SERVER_NAME='$SERVER_NAME'"
USER_NAME=$(whoami)
echo "USER_NAME='$USER_NAME'"
DIR_NAME=$(pwd)
echo "DIR_NAME='$DIR_NAME'"

# TODO(gp): Generalize this to all users.
if [[ $USER_NAME == "saggese" ]]; then
  if [[ 1 == 1 ]]; then
    # TODO: It should be function of current dir.
    IP_NAME="localhost"
    if [[ $DIR_NAME == "/Users/gp/src/git_particleone_teza1" ]]; then
      PORT=8101
    else
      echo "ERROR: Dir '$DIR_NAME' not recognized"
      exit -1
    fi;
  else
    echo "ERROR: System '$SERVER_NAME' ($OS_NAME) not recognized"
    exit -1
  fi;
else
  echo "ERROR: Invalid user '$USER_NAME'. Add your credentials to this script"
  exit -1
fi;

echo "You can connect to: $IP_NAME:$PORT"

jupyter notebook '--ip=*' --browser chrome . --port $PORT
