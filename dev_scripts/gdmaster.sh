#!/bin/bash -e

# ```
# Diff current branch with master.
# ```

source helpers.sh

execute "git fetch"

echo "==================== master - branch ===================="
execute "gll ..origin/master"

echo
echo "==================== branch - master ===================="
execute "gll origin/master.."
