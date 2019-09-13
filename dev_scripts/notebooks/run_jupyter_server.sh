#!/bin/bash -e

# First optional param is the conda env.
if [[ -z $1 ]]; then
  ENV="develop"
else
  ENV=$1
fi;
echo "ENV=$ENV"

# Second optional param is the port number.
if [[ -z $2 ]]; then
  PORT=9999
else
  PORT=$2
fi;
echo "PORT=$PORT"

# Config env.
source $SRC_DIR/utilities/dev_scripts/setenv.sh -t $ENV

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
  if [[ $SERVER_NAME == "gpmac.lan" ]]; then
    # TODO: It should be function of current dir.
    IP_NAME="localhost"
    if [[ $DIR_NAME == "/Users/gp/src/git_particleone" ]]; then
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
