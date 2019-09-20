#!/bin/bash -e

# ```
# Force a revert of files to HEAD.
# ```

source helpers.sh

exec "git reset HEAD $*"
exec "git checkout -- $*"
