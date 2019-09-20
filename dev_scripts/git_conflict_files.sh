#!/bin/bash -e

# ```
# Find files with git conflicts.
# ```

source helpers.sh

exec "git diff --name-only --diff-filter=U"
