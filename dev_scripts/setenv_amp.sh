#!/bin/bash -e

# """
# This file needs to be sourced:
#   > source setenv.sh
# instead of executed:
#   > ./setenv.sh
# in order to set the env vars in the calling shell.
#
# All the parameters passed to this script are propagated to _setenv.py
# """

# Name of the current script, e.g,. `./dev_scripts/setenv.sh`
EXEC_NAME="${BASH_SOURCE[0]}"
echo "Running '$EXEC_NAME' ..."
# Dir to the executable, e.g., `./dev_scripts`
EXEC_DIR=$(dirname "$EXEC_NAME")
# Full path to the executable, e.g.,
#   `/Users/saggese/src/commodity_research1/amp/dev_scripts`
EXEC_PATH=$(cd $EXEC_DIR; pwd -P)
echo "EXEC_DIR=$EXEC_DIR"
echo "EXEC_PATH=$EXEC_PATH"

if [[ -z $1 ]]; then
    ENV_NAME="amp_develop"
else
    ENV_NAME=$2
fi

# source helpers.sh
HELPERS_PATH=$EXEC_PATH/helpers.sh
echo "HELPERS_PATH=$HELPERS_PATH"
if [[ ! -e $HELPERS_PATH ]]; then
    echo "File '$HELPERS_PATH' doesn't exist: exiting"
    return 1
fi
source $HELPERS_PATH

# Check python version.
get_python_version
rc=$?
if [[ $rc != 0 ]]; then
  echo "python is too old: exiting"
  return $rc
fi;

# Create and execute the setenv.sh script to configure the env.
execute_setenv "_setenv_amp" $ENV_NAME
