#!/usr/bin/env bash

# - When we run pre-commit in a Git submodule we can't mount the submodule
#   directory in Docker
#   - This is because pre-commit gets confused by the fact that the directory
#     looks like a Git repo, but `.git` is not the Git directory but only a
#     file:
#     ```
#     > more .git
#     gitdir: ../.git/modules/amp
#     ```
#
# - To work around this problem we mount the entire Git super-project
#   repository and then we tweak the `WORK_DIR` to change the working directory
#   to the submodule. See PartTask6350 for more context.
# - We have filed an issue in pre-commit official repo
#   (https://github.com/pre-commit/pre-commit/issues/1734), but they won't
#   going to fix this

# Return the path to the Git repo including the Git submodule for a submodule
# and it's empty for a supermodule.
SUBMODULE_SUPERPROJECT=$(git rev-parse --show-superproject-working-tree)

if [ $SUBMODULE_SUPERPROJECT ]; then
    # E.g., `amp`.
    SUBMODULE_NAME=$(git config \
    --file $SUBMODULE_SUPERPROJECT/.gitmodules \
    --get-regexp path \
    | grep $(basename "$(pwd)")$ \
    | awk '{print $2}')
    echo "Running pre-commit for the Git $SUBMODULE_NAME submodule."
    # The working dir is the submodule.
    WORK_DIR="/src/$SUBMODULE_NAME"
    REPO_ROOT=$SUBMODULE_SUPERPROJECT
  else
    WORK_DIR="/src"
    REPO_ROOT="$(pwd)"
fi

#IMAGE="083233266530.dkr.ecr.us-east-2.amazonaws.com/dev_tools_tmp:prod"
IMAGE="083233266530.dkr.ecr.us-east-2.amazonaws.com/dev_tools:prod"
echo "IMAGE=$IMAGE"

# Keep this in sync with devops/compose/docker-compose-user-space.yml.
cmd="docker run --rm -t \
--env \"SKIP=$SKIP\" \
-v \"$REPO_ROOT\":/src \
-v \"$REPO_ROOT/.pre-commit-config.yaml\":/app/.pre-commit-config.yaml \
--workdir=$WORK_DIR \
$IMAGE \
\"/usr/local/bin/pre-commit $@\""

# We use /root/.cache/pre-commit has cache so it's in the container and not
# shared with the user, so we don't need the following:
#   --env \"PRE_COMMIT_HOME=$HOME/.cache/pre-commit\" \
#   -v \"$HOME/.cache:/app/.cache\" \

echo "> $cmd"
eval $cmd
