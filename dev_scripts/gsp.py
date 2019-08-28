#!/usr/bin/env python

"""
Stash the changes in a Git client without changing the client, besides a reset
of the inex.
"""

import argparse
import logging

import helpers.dbg as dbg
import helpers.git as git
import helpers.printing as pri
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# ##############################################################################


def _system(cmd, *args, **kwargs):
    si.system(cmd, log_level=logging.INFO, *args, **kwargs)


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level)
    #
    msg = "# Saving local changes..."
    print("\n" + pri.frame(msg))
    tag, was_stashed = git.git_stash_save(prefix="gup", log_level=logging.INFO)
    print("tag='%s'" % tag)
    if not was_stashed:
        # raise RuntimeError(msg)
        pass
    else:
        msg = "# Restoring local changes..."
        print("\n" + pri.frame(msg))
        git.git_stash_apply(mode="apply", log_level=logging.INFO)


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
