#!/usr/bin/env python
"""
- This script is equivalent to git commit -am "..."
- Perform various checks on the git client.
"""

import argparse
import logging
import os
import sys

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.printing as pri
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# ##############################################################################

_ALL_ACTIONS = [
    "check_commit_message",
    "check_user_name",
    "linter",
    "run_tests",
    "commit",
]


# TODO(gp): Share with linter.py
def _actions_to_string(actions):
    actions_as_str = [
        "%24s: %s" % (a, "Yes" if a in actions else "-") for a in _ALL_ACTIONS
    ]
    return "\n".join(actions_as_str)


def _update_action(action, actions):
    is_present = action in actions
    actions_out = actions[:]
    if is_present:
        actions_out = [a for a in actions_out if a != action]
    return is_present, actions_out


def _select_phases(args):
    # Select phases.
    actions = args.action
    if isinstance(actions, str) and " " in actions:
        actions = actions.split(" ")
    if not actions or args.all:
        actions = _ALL_ACTIONS[:]
    # Validate actions.
    actions = set(actions)
    for action in actions:
        if action not in _ALL_ACTIONS:
            raise ValueError("Invalid action '%s'" % action)
    # Reorder actions according to _ALL_ACTIONS.
    actions_tmp = []
    for action in _ALL_ACTIONS:
        if action in actions:
            actions_tmp.append(action)
    actions = actions_tmp
    # Print actions.
    actions_as_str = _actions_to_string(actions)
    _LOG.info("\n# Action selected:\n%s", pri.space(actions_as_str))
    return actions


# ##############################################################################


def _check_commit_message(args, actions):
    action = "check_commit_message"
    is_present, actions = _update_action(action, actions)
    commit_file = commit_msg = None
    if is_present:
        commit_file = "tmp.commit.txt"
        commit_msg = args.m
        commit_msg = commit_msg.rstrip("\n") + "\n\n"
        io_.to_file(commit_file, commit_msg)
        # TODO(gp): Check commit message.
    return actions, commit_file, commit_msg


def _run_unit_tests(args, actions, commit_file, commit_msg):
    action = "run_tests"
    is_present, actions = _update_action(action, actions)
    unit_test_passing = True
    if is_present:
        cmd = "run_tests.py"
        if args.test:
            cmd = 'pytest edgar -k "TestIsUnicodeDash"'
        print(pri.frame(cmd, char1="#"))
        rc = si.system(cmd, suppress_output=False, abort_on_error=False)
        unit_test_passing = rc == 0
        msg = "Unit tests passing: %s" % (
            "Yes" if unit_test_passing else "*** NO ***"
        )
        _LOG.info("%s", msg)
        io_.to_file(commit_file, msg, mode="a")
        commit_msg += msg
        # Handle errors.
        print(pri.frame("Commit results", char1="#"))
        _LOG.info("%s", commit_msg)
        if not unit_test_passing:
            if not args.not_abort_on_error:
                _LOG.error(
                    "Exiting. If you don't want to abort on errors use "
                    "--not_abort_on_error"
                )
                sys.exit(-1)
            else:
                _LOG.warning("Continue despite unit tests failing")
    return actions, unit_test_passing


def _linter(args, actions, commit_file, commit_msg):
    action = "linter"
    is_present, actions = _update_action(action, actions)
    num_lints = 0
    if is_present:
        cmd = "linter.py"
        if args.test:
            cmd = "linter.py --action isort"
        print(pri.frame(cmd, char1="#"))
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
                    "Exiting. If you don't want to abort on errors use "
                    "--not_abort_on_error"
                )
                sys.exit(-1)
            else:
                _LOG.warning("Continue despite linter errors")
    return actions, commit_msg, num_lints


def _check_user_name(actions):
    action = "check_user_name"
    is_present, actions = _update_action(action, actions)
    if is_present:
        # Keep this in sync with dev_scripts/setenv.sh
        _valid_users = ["saggese", "Paul"]
        user_name = git.get_git_name()
        if user_name not in _valid_users:
            _LOG.error(
                "Invalid git name '%s': valid git names are %s",
                user_name,
                _valid_users,
            )
            sys.exit(-1)
        # TODO(gp): Check email with dev_scripts/setenv.sh
    return actions


def _commit(args, commit_file, num_lints, unit_test_passing):
    # TODO(gp): The flow is:
    #   git commit in all submodules
    #   git submodule update --remote
    #   git add amp
    #   gp in all submodules
    #   git commit
    # Generate commit message in a file.
    if False and not args.force_commit:
        if num_lints != 0:
            msg = "Found %d linter errors" % num_lints
            _LOG.warning(msg)
        if unit_test_passing:
            msg = "Unit tests are not passing: you should not commit"
            _LOG.warning(msg)
    if args.commit:
        cwd = os.getcwd()
        _LOG.info("cwd=%s", cwd)
        # TODO(gp): We should query git.
        submodules = "amp".split()
        for submod in submodules:
            cmd = "cd %s && git commit --file %s" % (submod, commit_file)
            si.system(cmd)
        #
        for submod in submodules:
            cmd = "git commit --file %s" % commit_file
            si.system(cmd)
            cmd = "gup.py"
            si.system(cmd)
            cmd = "git push"
            si.system(cmd)
    else:
        msg = "\nCommit with:\n> git commit --file %s" % commit_file
        _LOG.info("%s", msg)


# ##############################################################################


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--action",
        action="append",
        choices=_ALL_ACTIONS,
        help="Run certain phases",
    )
    parser.add_argument(
        "-m",
        "--message",
        required=True,
        action="store",
        type=str,
        help="Commit message",
    )
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--not_abort_on_error", action="store_true")
    parser.add_argument("--force_commit", action="store_true")
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    actions = _select_phases(args)
    # TODO(gp): Make sure that index is empty.
    # Check commit message
    actions, commit_file, commit_msg = _check_commit_message(args, actions)
    # TODO(gp):
    #  1) Did you make sure that the external dependencies are minimized?
    #  2) Is the code properly unit tested?
    # TODO(gp): git diff check?
    # Check user name.
    actions = _check_user_name(actions)
    # Run linter.
    actions, commit_msg, num_lints = _linter(
        args, actions, commit_file, commit_msg
    )
    # Run tests.
    actions, unit_test_passing = _run_unit_tests(
        args, actions, commit_file, commit_msg
    )
    # Commit.
    _commit(args, commit_file, num_lints, unit_test_passing)


if __name__ == "__main__":
    _main(_parse())
