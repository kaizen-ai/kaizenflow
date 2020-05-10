#!/usr/bin/env python

import argparse
import itertools
import logging
import os
import py_compile
import re
import sys
from typing import Any, List, Tuple, Type

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.list as hlist
import helpers.parser as prsr
import helpers.printing as prnt
import helpers.system_interaction as si
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)

def _calculate_stats(data_pull_request_base_sha: str, data_pull_request_head_ref, data_pull_request_head_sha):

# # Calculate stats
# files_changed_in_branch=$(git diff --name-only ${data_pull_request_base_sha}...)
# echo "Files changed in branch: ${files_changed_in_branch}."

    dir_name = "."
    # TODO: Think about it.
    remove_files_non_present = True
    mod_files = git.get_modified_files_in_branch(
        dir_name, data_pull_request_base_sha, remove_files_non_present=remove_files_non_present)
    #_LOG.info("modirty: %s", master_dirty)

# # Calculate "After*" stats
# # Suppress all errors, we handle them on upper level.
# set +e
# linter.py -t ${data_pull_request_base_sha} --post_check
# branch_dirty=$?
# echo "Branch dirty: ${branch_dirty}."
#
# git reset --hard
# linter.py -t ${data_pull_request_base_sha}
# branch_lints=$?
# echo "Lints in branch: ${branch_lints}."
#
# # Read lints in memory
# lints_message="\`\`\`\n"
#
# while IFS= read -r line
# do
# lints_message="${lints_message}${line}\n"
# done <./linter_warnings.txt
#
# lints_message="${lints_message}\n\`\`\`"

    cmd = f"linter.py -t ${data_pull_request_base_sha} --post_check"
    branch_dirty = si.system(cmd, abort_on_error=False)
    _LOG.info("Branch dirty: %s", branch_dirty)
    #
    cmd = f"git reset --hard"
    si.system(cmd)
    #
    cmd = f"linter.py -t ${data_pull_request_base_sha}"
    branch_lints = si.system(cmd, abort_on_error=False)
    _LOG.info("Branch lints: %s", branch_lints)
    #
    linter_output_filename = "./linter_warnings.txt"
    # TODO: Rename -> linter_message
    lints_message = io_.from_file(linter_output_filename)
    lints_message = "```\n" + txt + "```\n"

# # Calculate "Before*" stats
# git reset --hard
# git checkout ${data_pull_request_base_sha} --recurse-submodules
# linter.py --files $files_changed_in_branch --post_check
# master_dirty=$?
# echo "Master dirty: ${master_dirty}."
#
# git reset --hard
# linter.py --files ${files_changed_in_branch}
# master_lints=$?
# echo "Lints in master: ${master_lints}."

    # # Calculate "Before*" stats
    cmd = f"git reset --hard"
    si.system(cmd)
    cmd = f"git checkout ${data_pull_request_base_sha} --recurse-submodules"
    si.system(cmd)
    mod_files_as_str = " ".join(mod_files)
    cmd = f"linter.py --files {mod_files_as_str} --post_check"
    master_dirty = si.system(cmd, abort_on_error=False)
    _LOG.info("Master dirty: %s", master_dirty)
    #
    cmd = f"git reset --hard"
    si.system(cmd)
    cmd = f"linter.py --files {mod_files_as_str}"
    master_lints = si.system(cmd, abort_on_error=False)
    _LOG.info("Master lints: %s", master_lints)

# # Prepares a message and exit status
# master_dirty_status="False"
# if [[ "$master_dirty" -gt 0 ]] ; then
# master_dirty_status="True"
# fi

    master_dirty_status = master_dirty > 0
#
# exit_status=0
# branch_dirty_status="False"
# errors=""
# if [[ "$branch_dirty" -gt 0 ]] ; then
# branch_dirty_status="True"
# exit_status=1
# errors="${errors}**ERROR**: Run \`linter.py. -b\` locally before merging."
# fi

    exit_status = 0
    errors = []

    branch_dirty_status = branch_dirty > 0
    if branch_dirty_status:
        errors.append("**ERROR**: Run \`linter.py. -b\` locally before merging.")

# if [[ "$master_lints" -gt 0 ]] ; then
# errors="${errors}\n**WARNING**: Your branch has lints. Please fix them."
# fi

    if master_lints > 0:
        errors.append("**WARNING**: Your branch has lints. Please fix them.")
#
# if [[ "$branch_lints" -gt "$master_lints"  ]] ; then
# exit_status=1
# errors="${errors}\n**ERROR**: You introduced more lints. Please fix them."
# fi

    if branch_lints > master_lints:
        exit_status = 1
        errors.append("**ERROR**: You introduced more lints. Please fix them.")

#
# message="\n# Results of the linter build"
# message="${message}\nConsole output: ${BUILD_URL}console"
# message="${message}\n- Master (sha: ${data_pull_request_base_sha})"
# message="${message}\n   - Number of lints: ${master_lints}"
# message="${message}\n   - Dirty (i.e., linter was not run): ${master_dirty_status}"
# message="${message}\n- Branch (${data_pull_request_head_ref}: ${data_pull_request_head_sha})"
# message="${message}\n   - Number of lints: ${branch_lints}"
# message="${message}\n   - Dirty (i.e., linter was not run): ${branch_dirty_status}"
# message="${message}\n\nThe number of lints introduced with this change: $(expr ${branch_lints} - ${master_lints})"
#
# message="${message}\n\n${errors}"
#
# message="${message}\n${lints_message}\n"

    message.append("# Results of the linter build")
    message.append(f"Console output: ${BUILD_URL}console")
    message.append(f"- Master (sha: ${data_pull_request_base_sha})")
    message.append(f"- Number of lints: ${master_lints}")
    message.append(f"- Dirty (i.e., linter was not run): ${master_dirty_status}")
    message.append(f"- Branch (${data_pull_request_head_ref}: ${data_pull_request_head_sha})")
    message.append(f"- Number of lints: ${branch_lints}")
    message.append(f"- Dirty (i.e., linter was not run): ${branch_dirty_status}")
    diff_lints = branch_lints - master_lints
    message.append(f"\nThe number of lints introduced with this change: {diff_lints}")
    message = "\n".join(message)
    message += "\n\n" + "\n".join(errors)
    message += "\n" + lints_message
    #
    # message="${message}\n\n${errors}"
    #
    # message="${message}\n${lints_message}\n"
    return exit_status, message


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Select files.
    parser.add_argument(
        "--jenkins",
        action="store_true",
        help="",
    )
    prsr.add_verbosity_arg(parser)
    return parser


if __name__ == "__main__":
    parser_ = _parse()
    args_ = parser_.parse_args()
    rc_ = _main(args_)
    if args.jenkins:
        data_pull_request_base_sha = os.environ["data_pull_request_base_sha"]
        data_pull_request_head_ref = os.environ["data_pull_request_head_ref"]
        data_pull_request_head_sha = os.environ["data_pull_request_head_sha"]
        #printf "${message}" > ./tmp_message.txt
        #printf "${exit_status}" > ./tmp_exit_status.txt
        pass
    sys.exit(rc_)
