#!/usr/bin/env python

"""
Run
"""

import argparse
import logging
import os
from typing import List

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.printing as prnt
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# #############################################################################


def _get_changed_files(dst_branch: str) -> List[str]:
    cmd = "git diff --name-only %s..." % dst_branch
    _, output = si.system_to_string(cmd)
    file_names = si.get_non_empty_lines(output)
    return file_names


def _qualify_branch(
    tag: str, dst_branch: str, test_list: str, quick: bool
) -> List[str]:
    print(prnt.frame("Qualifying '%s'" % tag))
    output = []
    # - Linter.
    output.append(prnt.frame("%s: linter log" % tag))
    file_names = _get_changed_files(dst_branch)
    _LOG.debug("file_names=%s", file_names)
    if not file_names:
        _LOG.warning("No files different in %s", dst_branch)
    else:
        output.append("Files modified:\n%s" % prnt.space("\n".join(file_names)))
        linter_log = "./%s.linter_log.txt" % tag
        linter_log = os.path.abspath(linter_log)
        cmd = "linter.py -f %s --linter_log %s" % (
            " ".join(file_names),
            linter_log,
        )
        si.system(cmd, suppress_output=False)
        # Read output from the linter.
        txt = io_.from_file(linter_log)
        output.append(txt)
    # - Run tests.
    if True:
        output.append(prnt.frame("%s: unit tests" % tag))
        if quick:
            cmd = "pytest -k Test_p1_submodules_sanity_check1"
        else:
            cmd = "run_tests.py --test %s --num_cpus -1" % test_list
        output.append("cmd line='%s'" % cmd)
        si.system(cmd, suppress_output=False)
    #
    return output


# #############################################################################


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--src_branch",
        action="store",
        default=None,
        help="Name of the branch to merge. No value means use "
        "the branch we are currently in",
    )
    parser.add_argument(
        "--dst_branch",
        action="store",
        default="master",
        help="Branch to merge into, typically " "master",
    )
    parser.add_argument("--test_list", action="store", default="slow")
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--merge_if_successful", action="store_true")
    parser.add_argument(
        "--summary_file", action="store", default="./summary_file.txt"
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    #
    output = []
    # Update the src branch.
    if args.src_branch is not None:
        cmd = "git checkout %s" % args.src_branch
        si.system(cmd)
    # If this is master, then raise an error.
    branch_name = git.get_branch_name()
    _LOG.info("Current branch_name: %s", branch_name)
    msg = "Merging: %s -> %s" % (branch_name, args.dst_branch)
    output.append(msg)
    if True:
        dbg.dassert_ne(branch_name, "master", "You can't merge from master")
    # TODO(gp): Make sure the Git client is empty.
    # Update the dst branch.
    if True:
        cmd = "git fetch origin %s:%s" % (args.dst_branch, args.dst_branch)
        si.system(cmd)
    # Refresh.
    def _refresh(dst_dir):
        _LOG.debug("Refreshing dst_dir=%s", dst_dir)
        cd_cmd = "cd %s && " % dst_dir
        if False:
            # Make a backup.
            msg = "git_merge_branch.py"
            cmd = "git stash save --keep-index '%s' && git stash apply" % msg
            cmd = cd_cmd + cmd
            si.system(cmd)
        # Pull.
        cmd = "git pull"
        cmd = cd_cmd + cmd
        si.system(cmd)
        # Merge master.
        cmd = "git merge master --commit --no-edit"
        cmd = cd_cmd + cmd
        si.system(cmd)

    # Refresh curr repo.
    _refresh(".")
    # Refresh amp repo, if needed.
    if os.path.exists("amp"):
        _refresh("amp")
    # Qualify amp repo.
    if os.path.exists("amp"):
        tag = "amp"
        output_tmp = _qualify_branch(tag, args.dst_branch, args.test_list)
        output.extend(output_tmp)
    #
    repo_sym_name = git.get_repo_symbolic_name(super_module=True)
    _LOG.info("repo_sym_name=%s", repo_sym_name)
    # Qualify current repo.
    tag = "curr"
    output_tmp = _qualify_branch(tag, args.dst_branch, args.test_list, args.quick)
    output.extend(output_tmp)
    # Qualify amp repo, if needed.
    if os.path.exists("amp"):
        tag = "amp"
        output_tmp = _qualify_branch(
            tag, args.dst_branch, args.test_list, args.quick
        )
        output.extend(output_tmp)
    # Forward amp.

    # Report the output.
    output_as_txt = "\n".join(output)
    io_.to_file(args.summary_file, output_as_txt)
    # print(output_as_txt)
    _LOG.info("Summary file saved into '%s'", args.summary_file)

    # Merge.
    # TODO(gp): Add merge step.


if __name__ == "__main__":
    _main(_parse())
