#!/bin/bash -e

# """
# Delete all the temporary files related to running.
# """

FILES=""
FILES_TMP="find . -name '__pycache__'"
FILES="$FILES $FILES_TMP"
FILES_TMP="find . -name '.pytest_cache' -type d"
FILES="$FILES $FILES_TMP"
FILES_TMP="find . -name '*.pyc'"
FILES="$FILES $FILES_TMP"

# git status --short --ignored | grep -v .idea

echo $FILES
