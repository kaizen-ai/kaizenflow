#!/bin/bash -e

# """
# - No conda env is built, but we rely on `p1_develop.daily_build` being already
#  build.
# - This script runs the linter on a git branch.
# """


CONDA_ENV="amp_develop.daily_build"
DEV_SCRIPTS_DIR="./dev_scripts_p1"

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Init.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

CMD="source ./dev_scripts/jenkins/amp.jenkins_helpers.sh"
echo "+ $CMD"
eval $CMD

source_scripts

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Setenv.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

setenv "$AMP/dev_scripts/setenv_amp.sh" $CONDA_ENV

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Run linter.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# Calculate stats
files_changed_in_branch=$(git diff --name-only ${data_pull_request_base_sha}...)
echo "Files changed in branch: ${files_changed_in_branch}."

# Calculate "After*" stats
# Suppress all errors, we handle them on upper level.
set +e
linter.py -b --post_check
branch_dirty=$?
echo "Branch dirty: ${branch_dirty}."

git reset --hard
linter.py -b
branch_lints=$?
echo "Lints in branch: ${branch_lints}."

# Calculate "Before*" stats
git checkout ${data_pull_request_base_sha}
git reset --hard
linter.py --files $files_changed_in_branch --post_check
master_dirty=$?
echo "Master dirty: ${master_dirty}."

git reset --hard
linter.py --files ${files_changed_in_branch}
master_lints=$?
echo "Lints in master: ${branch_lints}."

# Prepares a message and exit status
message="## Automatically generated report (${JOB_NAME})\nConsole url: ${console_url}\n"
exit_status=0

if [[ "$master_lints" -lt "$branch_lints" ]] ; then
  message=${message}"Number of lints were increased."
  exit_status=1
fi
message=${message}" master_lints: $master_lints, branch_lints: $branch_lints.\n"

if [[ "master_dirty" -gt 0 ]] ; then
  message=${message}"Branch \`master(sha: ${data_pull_request_base_sha})\` was dirty after linter.\n"
fi

if [[ "$branch_dirty" -gt 0 ]] ; then
  message=${message}"Branch `data_pull_request_head_ref` is dirty after linter."
  exit_status=1
fi
