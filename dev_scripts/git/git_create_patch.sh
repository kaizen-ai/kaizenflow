#!/bin/bash -e

# """
# Create a patch file for the entire repo client from the base revision.
# This script accepts a list of files to package, if specified.
# """

# TODO(gp): Improve this.

echo "Creating patch ..."
HASH_=$(git rev-parse --short HEAD)
TIMESTAMP=$(timestamp)
GIT_ROOT=$(git rev-parse --show-toplevel)
BASENAME=$(basename $GIT_ROOT)
echo "GIT_ROOT=$GIT_ROOT"

cd $GIT_ROOT
DST_FILE="$git_root/patch.$BASENAME.$HASH_.$TIMESTAMP.txt"
echo "DST_FILE=$DST_FILE"

git status -s $*
# Find all files modified (instead of --cached).
git diff HEAD $* >$DST_FILE

echo
echo "To apply the patch and execute:"
echo "> git checkout $HASH_"
echo "> git apply "$DST_FILE

# Remote patch.
echo
echo "Remote patch:"
FILE=$(basename $DST_FILE)
#
SERVER="server"
CLIENT_PATH="~/src/"
echo "> scp $FILE $SERVER: && ssh $SERVER 'cd $CLIENT_PATH && git apply ~/$FILE'"
