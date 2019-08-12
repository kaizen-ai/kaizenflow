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
import helpers.io_ as io_
import helpers.printing as print_
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# ##############################################################################


_ACTIONS = [
        "check_commit_message",
        "check_user_name",
        #"linter",
        #"run_tests",
        ]


def _update_action(action, actions):
    is_present = action in actions
    actions_out = actions[:]
    if is_present:
        actions_out = [a for a in actions_out if a != action]
    return is_present, actions_out


def _main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-m", required=True, action="store", type=str, help="Commit message")
    parser.add_argument("--commit", action="store_true")
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
    actions = _ACTIONS[:]
    # TODO(GP): Make sure that index is empty.
    #
    # Check commit message
    #
    action = "check_commit_message"
    is_present, actions_out = _update_action(action, actions)
    if is_present:
        commit_file = "tmp.commit.txt"
        commit_msg = args.m
        commit_msg = commit_msg.rstrip("\n") + "\n\n"
        io_.to_file(commit_file, commit_msg)
        # TODO(GP): Check commit message.
    # TODO(GP):
    # 1) Did you make sure that the external dependencies are minimized?
    # 3) Is the code properly unit tested?
    # TODO(GP): git diff check?
    #
    # Check user name.
    #
    action = "check_user_name"
    is_present, actions_out = _update_action(action, actions)
    if is_present:
        # Keep this in sync with dev_scripts/setenv.sh
        _valid_users = [
            "saggese",
            "Paul",
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
    action = "linter"
    is_present, actions_out = _update_action(action, actions)
    if is_present:
        cmd = "linter.py"
        if args.test:
            cmd = "linter.py --action isort"
        print(print_.frame(cmd, char1="#"))
        num_lints = si.system(cmd, suppress_output=False, abort_on_error=False)
        # Post message.
        msg = "Num lints: %s\n" % num_lints
        _LOG.info("%s", msg)
        io_.to_file(commit_file, msg, mode="a")
        commit_msg += msg
        # Handle errors.
        if num_lints != 0:
            if not args.not_abort_on_error:
                _LOG.error(
                    "Exiting. If you don't want to abort on errors use --not_abort_on_error"
                )
                sys.exit(-1)
            else:
                _LOG.warning("Continue despite linter errors")
    #
    # Run tests.
    #
    action = "run_tests"
    is_present, actions_out = _update_action(action, actions)
    if is_present:
        cmd = "run_tests.py"
        if args.test:
            cmd = 'pytest edgar -k "TestIsUnicodeDash"'
        print(print_.frame(cmd, char1="#"))
        rc = si.system(cmd, suppress_output=False, abort_on_error=False)
        unit_test_passing = rc == 0
        msg = "Unit tests passing: %s" % ("Yes"
                                          if unit_test_passing else "*** NO ***")
        _LOG.info("%s", msg)
        io_.to_file(commit_file, msg, mode="a")
        commit_msg += msg
        # Handle errors.
        print(print_.frame("Commit results", char1="#"))
        _LOG.info("%s", commit_msg)
        if not unit_test_passing:
            if not args.not_abort_on_error:
                _LOG.error(
                    "Exiting. If you don't want to abort on errors use --not_abort_on_error"
                )
                sys.exit(-1)
            else:
                _LOG.warning("Continue despite unit tests failing")
    #
    # Generate commit message in a file.
    #
    if False and not args.force_commit:
        if num_lints != 0:
            msg = "Found %d linter errors" % num_lints
            _LOG.warning(msg)
        if unit_test_passing:
            msg = "Unit tests are not passing: you should not commit"
            _LOG.warning(msg)
    if args.commit:
        cmd = "git commit --file %s" % commit_file
        _LOG.info("cmd=%s", cmd)
        cmd = "gup"
        _LOG.info("cmd=%s", cmd)
        cmd = "git push"
        _LOG.info("cmd=%s", cmd)
    else:
        _LOG.info("\nCommit with:\n> git commit --file %s" % commit_file)



if __name__ == '__main__':
    _main()
