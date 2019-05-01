#!/bin/bash
#
# Run yapf on the modified files.
#
files=$(git ls-files -m | egrep ".py$")
echo $files | xargs -t -n 1 isort
echo $files | xargs -t -n 1 yapf.sh -i
