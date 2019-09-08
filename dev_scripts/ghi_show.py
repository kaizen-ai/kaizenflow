#!/usr/bin/env python

"""
"""

import argparse
import logging
import os

import helpers.dbg as dbg
import helpers.printing as pri
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# ##############################################################################


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("positional", nargs=1, help="Github issue number")
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    return parser


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level, use_exec_path=True)
    #
    dbg.dassert_eq(len(args.positional), 1)
    issue_num = int(args.positional[0])
    dbg.dassert_lte(1, issue_num)
    #
    print(pri.color_highlight("# Github:", "red"))
    cmd = "ghi show %d | head -1" % issue_num
    _, txt = si.system_to_string(cmd)
    print(txt)
    print(
        "https://github.com/ParticleDev/commodity_research/issues/%d" % issue_num
    )
    #
    print(pri.color_highlight("# Files in the repo:", "red"))
    # regex="Task${*}*\.*py*"
    regex = "*${*}*\.*py*"
    _LOG.debug("regex=%s", regex)
    cmd = "find . -name %s | grep -v ipynb_checkpoint" % regex
    _, txt = si.system_to_string(cmd)
    print(txt)
    #
    env_name = "P1_GDRIVE_PATH"
    if env_name in os.environ:
        dir_name = os.environ["env_name"]
        print(
            pri.color_highlight("# Files in the gdrive '%s':" % dir_name, "red")
        )
        regex = "*${*}*"
        _LOG.debug("regex=%s", regex)
        cmd = "find %s -name %s" % (dir_name, regex)
        _, txt = si.system_to_string(cmd)
        print(txt)
    else:
        _LOG.warning("No env var '%s' so can't look for files there", env_name)


if __name__ == "__main__":
    _main(_parse())
