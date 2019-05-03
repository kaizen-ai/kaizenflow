#!/usr/bin/env python
"""
- This script is equivalent to git commit -am "..."
- Perform various checks on the git client.
"""

import argparse
import logging
import sys

import helpers.dbg as dbg
import helpers.git as git
import helpers.helper_io as io_
import helpers.printing as print_
import helpers.system_interaction as hsi

_LOG = logging.getLogger(__name__)

# ##############################################################################


def _main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-m", required=True, action="store", type=str, help="Commit message")
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--not_abort_on_error", action="store_true")
    parser.add_argument("--force_commit", action="store_true")
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")
    #
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level, use_exec_path=True)
    # TODO(GP): Make sure that index is empty.
    commit_file = "tmp.commit.txt"
    # TODO(GP): Check commit message.
    commit_msg = args.m
    commit_msg = commit_msg.rstrip("\n") + "\n\n"
    io_.to_file(commit_file, commit_msg)
    # TODO(GP):
    # 1) Did you make sure that the external dependencies are minimized?
    # 3) Is the code properly unit tested?
    # TODO(GP): git diff check?
    #
    # Check user name.
    #
    # Keep this in sync with dev_scripts/setenv.sh
    _valid_users = [
        "GP", "Paul",
    ]
    user_name = git.get_git_name()
    if user_name not in _valid_users:
        _LOG.error("Invalid git name '%s': valid git names are %s", user_name,
                   _valid_users)
        sys.exit(-1)
    # TODO(gp): Check email with dev_scripts/setenv.sh
    #
    # Run linter.
    #
    cmd = "linter.py"
    if args.test:
        cmd = "linter.py --action isort"
    print(print_.frame(cmd, char1="#"))
    num_lints = hsi.system(cmd, suppress_output=False, abort_on_error=False)
    # Post message.
    msg = "Num lints: %s\n" % num_lints
    print(msg)
    io_.to_file(commit_file, msg, mode="a")
    commit_msg += msg
    # Handle errors.
    if num_lints != 0:
        if not args.not_abort_on_error:
            print("Exiting. If you don't want to abort on errors use --not_abort_on_error")
            sys.exit(-1)
        else:
            _LOG.warning("Continue despite linter errors")
    #
    # Run tests.
    #
    cmd = "run_tests.py"
    if args.test:
        cmd = 'pytest edgar -k "TestIsUnicodeDash"'
    print(print_.frame(cmd, char1="#"))
    rc = hsi.system(cmd, suppress_output=False, abort_on_error=False)
    unit_test_passing = rc == 0
    msg = "Unit tests passing: %s" % ("Yes"
                                      if unit_test_passing else "*** NO ***")
    print(msg)
    io_.to_file(commit_file, msg, mode="a")
    commit_msg += msg
    # Handle errors.
    print(print_.frame("paranoid commit results", char1="#"))
    print(commit_msg)
    if not unit_test_passing:
        if not args.not_abort_on_error:
            print("Exiting. If you don't want to abort on errors use --not_abort_on_error")
            sys.exit(-1)
        else:
            _LOG.warning("Continue despite unit tests failing")
    #
    # Generate commit message in a file.
    #
    print("\nCommit with:\n> git commit --file %s" % commit_file)
    if not args.force_commit:
        if num_lints != 0:
            msg = "Found %d linter errors" % num_lints
            _LOG.warning(msg)
        if unit_test_passing:
            msg = "Unit tests are not passing: you should not commit"
            _LOG.warning(msg)


if __name__ == '__main__':
    _main()
