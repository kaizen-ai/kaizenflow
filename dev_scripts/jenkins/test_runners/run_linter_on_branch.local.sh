#!/bin/bash -e
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Prepare variables.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
parse_git_branch() {
 git branch 2> /dev/null | sed -e '/^[^*]/d' -e 's/* \(.*\)/\1/'
}
parse_commit_sha() {
 git log --pretty=format:'%h' -n 1
}

current_branch="$(parse_git_branch)"

data_pull_request_base_sha="master"
data_pull_request_head_ref="$(parse_commit_sha)"
BUILD_URL="The script was run locally. No "

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Run linter.
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

source amp/dev_scripts/jenkins/test_runners/run_linter_on_branch.sh 1>/dev/null


git stash &>/dev/null
echo ${current_branch}
git checkout ${current_branch} --recurse-submodules 1>/dev/null
echo -e ${message}