#!/bin/bash -e

if [[ -z $1 ]]; then
  ENV="develop"
else
  ENV=$1
fi;
echo "ENV=$ENV"

# Config env.
source $SRC_DIR/utilities/dev_scripts/setenv.sh -t $ENV

conda info --envs

if [[ -z $2 ]]; then
  PORT=9999
else
  PORT=$2
fi;
echo "PORT=$PORT"

jupyter notebook --ip=* --browser="chrome" . --port=$PORT
