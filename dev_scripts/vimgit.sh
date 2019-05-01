#!/bin/bash -e

FILES=$(git_files.sh)
echo "FILES=$FILES"
vim $FILES
