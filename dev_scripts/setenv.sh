#!/bin/bash -xe

# This file needs to be sourced:
#   > source setenv.sh
# instead of executed:
#   > ./setenv.sh
# in order to set the env vars in the calling shell.

EXEC_NAME="${BASH_SOURCE[0]}"
echo "Starting $EXEC_NAME ..."
DIR=$(dirname "$EXEC_NAME")
EXEC_PATH=$(cd $DIR ; pwd -P)

# Create the script to execute calling python.
DATETIME=$(date "+%Y%m%d-%H%M%S")
#DATETIME=""
SCRIPT_FILE=/tmp/setenv.${DATETIME}.sh
echo "SCRIPT_FILE=$SCRIPT_FILE"
$EXEC_PATH/_setenv.py --output_file $SCRIPT_FILE $*

# Execute the script
source $SCRIPT_FILE

echo "... $EXEC_NAME done"
