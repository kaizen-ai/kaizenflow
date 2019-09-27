#!/bin/bash -e

# ```
# Check out a branch and push it remotely.
# ```

source helpers.sh

execute "git checkout -b $*"
execute "git push -u origin $*"
