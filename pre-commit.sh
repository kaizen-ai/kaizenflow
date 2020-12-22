#!/usr/bin/env bash
# When we run pre-commit in a Git submodule we can't mount the submodule directory in Docker.
# This is because pre-commit gets confused by the fact that the directory looks like a submodule, but
# there is no including Git repository. To work around this problem we mount the entire Git super-project
# repository and then we tweak the `WORK_DIR` to change the working directory to the submodule.
# See PartTask6350 for more context.
# We file an issue in pre-commit official repo, but they won't going to fix this.
# https://github.com/pre-commit/pre-commit/issues/1734

if [ $(git rev-parse --show-superproject-working-tree) ]; then
    SUBMODULE_NAME=$(basename "$(pwd)")
    echo "Running linter for '$SUBMODULE_NAME' submodule."
    WORK_DIR="/src/$SUBMODULE_NAME"
    REPO_ROOT=$(git rev-parse --show-superproject-working-tree)
  else
    echo "Running linter for the repository."
    WORK_DIR="/src"
    REPO_ROOT="$(pwd)"
fi

docker run --rm -t \
    --env "PRE_COMMIT_HOME=$HOME/.cache/pre-commit" \
    --env "SKIP=$SKIP" \
    -v "$HOME/.cache:$HOME/.cache:rw" \
    -v "$REPO_ROOT":/src \
    --workdir "$WORK_DIR" \
    083233266530.dkr.ecr.us-east-2.amazonaws.com/dev_tools:prod "$@"
