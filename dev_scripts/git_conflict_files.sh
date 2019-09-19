#!/bin/bash -e

# ```
# Check out a branch and push it remotely.
# ```

source helpers.sh

exec "git diff --name-only --diff-filter=U"
