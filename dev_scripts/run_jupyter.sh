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

conda info --envs

echo "PYTHONPATH=$PYTHONPATH"
jupyter notebook --ip=* --browser="chrome" . --port=$PORT
