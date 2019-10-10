#!/bin/bash -e

# """
# Generate the files involved in a merge conflict.
# """

for FILE in $*; do
  echo "# Processing $FILE"
  git show :1:$FILE > $FILE.1_base
  git show :2:$FILE > $FILE.2_our
  git show :3:$FILE > $FILE.3_their
  ls $FILE.*
done;
