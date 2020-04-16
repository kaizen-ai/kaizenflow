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
git checkout ${data_pull_request_base_sha} --recurse-submodules
git reset --hard
linter.py --files $files_changed_in_branch --post_check
master_dirty=$?
echo "Master dirty: ${master_dirty}."

git reset --hard
linter.py --files ${files_changed_in_branch}
master_lints=$?
echo "Lints in master: ${master_lints}."

# Prepares a message and exit status
master_dirty_status="False"
if [[ "$master_dirty" -gt 0 ]] ; then
  master_dirty_status="True"
fi

exit_status=0
branch_dirty_status="False"
errors=""
if [[ "$branch_dirty" -gt 0 ]] ; then
    branch_dirty_status="True"
    exit_status=1
    errors="${errors}**ERROR**: You didn't run the linter. Please run it with \`linter.py. -b\`"
fi
if [[ "$master_lints" -gt 0 ]] ; then
    errors="${errors}\n**WARNING**: Your branch has lints. Please fix them."
fi

if [[ "$branch_lints" -gt "$master_lints"  ]] ; then
  exit_status=1
  errors="${errors}\n**ERROR**: You introduced more lints. Please fix them."
fi

message="\n# Results of the linter build\n- Master (sha: ${data_pull_request_base_sha})"
message="${message}\n   - Number of lints: ${master_lints}"
message="${message}\n   - Dirty (i.e., linter was not run): ${master_dirty_status}"
message="${message}\n- Branch (${data_pull_request_head_ref}: ${data_pull_request_head_sha})"
message="${message}\n   - Number of lints: ${branch_lints}"
message="${message}\n   - Dirty (i.e., linter was not run): ${branch_dirty_status}"
message="${message}\n\nThe number of lints introduced with this change: $(expr ${branch_lints} - ${master_lints})"

message="${message}\n\n${errors}"

# Disabled because of #1998
# Add outputs from linter_warnings.txt to the message.
message="${message}\n\`\`\`\n"

while IFS= read -r line
do
    message="${message}${line}\n"
done <./linter_warnings.txt

message="${message}\n\`\`\`"
printf "${message}" > ./tmp_message.txt

message_to_json() {
converter="\
import json
print(json.dumps({'body':open('./tmp_message.txt').read()}))
"
python -c "${converter}"
}
message="$(message_to_json)"
