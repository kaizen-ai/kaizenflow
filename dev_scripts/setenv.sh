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

execute_setenv

## Create the script to execute calling python.
#echo "Creating setenv script ... done"
#DATETIME=$(date "+%Y%m%d-%H%M%S")_$(python -c "import time; print(int(time.time()*1000))")
##DATETIME=""
#SCRIPT_FILE=/tmp/setenv.${DATETIME}.sh
#echo "SCRIPT_FILE=$SCRIPT_FILE"
#cmd="$EXEC_PATH/_setenv.py --output_file $SCRIPT_FILE $*"
#execute $cmd
#echo "Creating setenv script '$SCRIPT_FILE' ... done"
#
## Execute the newly generated script.
#echo "Sourcing '$SCRIPT_FILE' ..."
#source $SCRIPT_FILE
#echo "Sourcing '$SCRIPT_FILE' ... done"
#
#echo "Running '$EXEC_NAME' ... done"
