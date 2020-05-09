#!/bin/bash -e
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Run linter.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

# Calculate stats
files_changed_in_branch=$(git diff --name-only ${data_pull_request_base_sha}...)
echo "Files changed in branch: ${files_changed_in_branch}."

# Calculate "After*" stats
# Suppress all errors, we handle them on upper level.
set +e
linter.py -t ${data_pull_request_base_sha} --post_check
branch_dirty=$?
echo "Branch dirty: ${branch_dirty}."

git reset --hard
linter.py -t ${data_pull_request_base_sha}
branch_lints=$?
echo "Lints in branch: ${branch_lints}."

# Read lints in memory
lints_message="\`\`\`\n"

while IFS= read -r line
do
    lints_message="${lints_message}${line}\n"
done <./linter_warnings.txt

lints_message="${lints_message}\n\`\`\`"

# Calculate "Before*" stats
git reset --hard
git checkout ${data_pull_request_base_sha} --recurse-submodules
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
    errors="${errors}**ERROR**: Run \`linter.py. -b\` locally before merging."
fi
if [[ "$master_lints" -gt 0 ]] ; then
    errors="${errors}\n**WARNING**: Your branch has lints. Please fix them."
fi

if [[ "$branch_lints" -gt "$master_lints"  ]] ; then
  exit_status=1
  errors="${errors}\n**ERROR**: You introduced more lints. Please fix them."
fi

message="\n# Results of the linter build"
message="${message}\nConsole output: ${BUILD_URL}console"
message="${message}\n- Master (sha: ${data_pull_request_base_sha})"
message="${message}\n   - Number of lints: ${master_lints}"
message="${message}\n   - Dirty (i.e., linter was not run): ${master_dirty_status}"
message="${message}\n- Branch (${data_pull_request_head_ref}: ${data_pull_request_head_sha})"
message="${message}\n   - Number of lints: ${branch_lints}"
message="${message}\n   - Dirty (i.e., linter was not run): ${branch_dirty_status}"
message="${message}\n\nThe number of lints introduced with this change: $(expr ${branch_lints} - ${master_lints})"

message="${message}\n\n${errors}"

message="${message}\n${lints_message}\n"

