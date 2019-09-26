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

# Make sure conda works.
conda -V
rc=$?
if [[ $rc != 0 ]]; then
  echo "conda not working: exiting"
  return $rc
fi;

# Make sure python works.
python -V
rc=$?
if [[ $rc != 0 ]]; then
  echo "python not working: exiting"
  return $rc
fi;

# Check python version.
python - <<END
import sys
pyv = sys.version_info[:]
pyv_as_str = ".".join(map(str, pyv[:3]))
print("python version='%s'" % pyv_as_str)
is_too_old = pyv_as_str < "3.6"
print("is_too_old=%s" % is_too_old)
sys.exit(is_too_old);
END
rc=$?
if [[ $rc != 0 ]]; then
  echo "python is too old: exiting"
  return $rc
fi;

# Create the script to execute calling python.
DATETIME=$(date "+%Y%m%d-%H%M%S")
#DATETIME=""
SCRIPT_FILE=/tmp/setenv.${DATETIME}.sh
echo "SCRIPT_FILE=$SCRIPT_FILE"
$EXEC_PATH/_setenv.py --output_file $SCRIPT_FILE $*

# Execute the newly generated script.
source $SCRIPT_FILE

echo "... $EXEC_NAME done"
