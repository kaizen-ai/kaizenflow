#!/usr/bin/env python

"""
Update a git client by:
- stashing
- rebasing
- reapplying the stashed changes
"""

import argparse
import logging

import helpers.dbg as dbg
import helpers.git as git
import helpers.printing as pri
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

_LOG_LEVEL = "echo"

# ##############################################################################


def _system(cmd, *args, **kwargs):
    si.system(cmd, log_level=_LOG_LEVEL, *args, **kwargs)


def _print(msg):
    msg = pri.color_highlight(msg, "red")
    print("\n" + msg)


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level)
    #
    git_ll = "git log --date=local --oneline --graph --date-order --decorate"
    _print("# Checking what are the differences with master...")
    cmd = "%s ..origin/master" % git_ll
    _system(cmd, suppress_output=False)
    #
    cmd = "%s origin/master..." % git_ll
    _system(cmd, suppress_output=False)
    #
    _print("# Saving local changes...")
    tag, was_stashed = git.git_stash_push("gup", log_level=_LOG_LEVEL)
    print("tag='%s'" % tag)
    if not was_stashed:
        # raise RuntimeError(msg)
        pass
    #
    _print("# Getting new commits...")
    cmd = "git pull --rebase"
    _system(cmd, suppress_output=False)
    #
    if was_stashed:
        _print("# Restoring local changes...")
        git.git_stash_apply(mode="pop", log_level=_LOG_LEVEL)


def _parser():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    return parser


if __name__ == "__main__":
    _main(_parser())
