#!/usr/bin/env python

import argparse
import logging

import helpers.dbg as dbg
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
    msg = '# Checking what are the differences with master...'
    print("\n" + pri.frame(msg))
    cmd = "git ll ..origin/master"
    _system(cmd, suppress_output=False)
    #
    cmd = "git ll origin/master.."
    _system(cmd, suppress_output=False)
    #
    msg = '# Saving local changes...'
    print("\n" + pri.frame(msg))
    user_name = si.system_to_string("whoami")[1]
    server_name = si.system_to_string("uname -n")[1]
    date = si.system_to_string("date +%Y%m%d-%H%M%S")[1]
    tag = "gup.wip.%s-%s-%s" % (user_name, server_name, date)
    print("tag='%s'" % tag)
    cmd = "git stash save %s" % tag
    _system(cmd, suppress_output=False)
    # Check if we actually stashed anything.
    cmd = r"git stash list | \grep '%s' | wc -l" % tag
    output = si.system_to_string(cmd)[1]
    was_stashed = int(output) > 0
    if not was_stashed:
        msg = "Nothing was stashed"
        _LOG.warning(msg)
        #raise RuntimeError(msg)
    #
    msg = '# Getting new commits...'
    print("\n" + pri.frame(msg))
    cmd = 'git pull --rebase'
    _system(cmd, suppress_output=False)
    #
    if was_stashed:
        msg = '# Checking stash head ...'
        print("\n" + pri.frame(msg))
        cmd = "git stash list | head -3"
        _system(cmd, suppress_output=False)
        #
        msg = '# Restoring local changes...'
        print("\n" + pri.frame(msg))
        cmd = "git stash pop --quiet"
        _system(cmd, suppress_output=False)


def _parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")
    return parser


if __name__ == '__main__':
    _main(_parser())
