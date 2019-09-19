#!/bin/bash -e

# ```
# Diff current branch with master.
# ```

source helpers.sh

exec "git fetch"

echo "==================== master - branch ===================="
exec "gll ..origin/master"

echo
echo "==================== branch - master ===================="
exec "gll origin/master.."
