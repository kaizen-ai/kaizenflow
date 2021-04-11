#!/bin/bash -e

# """
# Create a patch file for the changes for `master...HEAD`.
# """

echo "Creating patch ..."
HASH_=$(git rev-parse --short HEAD)
TIMESTAMP=$(timestamp)
GIT_ROOT=$(git rev-parse --show-toplevel)
BASENAME=$(basename $GIT_ROOT)
echo "GIT_ROOT=$GIT_ROOT"

cd $GIT_ROOT
DST_FILE="$GIT_ROOT/patch.$BASENAME.$HASH_.$TIMESTAMP.tgz"
echo "DST_FILE=$DST_FILE"

FILES=$(git difftool --name-only master...)
echo $FILES

tar czvf $DST_FILE $FILES || true

echo
echo "To apply the patch execute:"
echo "> tar xvzf $DST_FILE"
