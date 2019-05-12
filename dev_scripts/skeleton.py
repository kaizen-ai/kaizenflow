#!/usr/bin/env python

import argparse
import logging
import os
import sys

#sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

import helpers.dbg as dbg
import helpers.system_interaction as si

_log = logging.getLogger(__name__)


# ##############################################################################


# TODO(gp): Use system param.
def _system(cmd, *args, **kwargs):
    print(("> %s" % cmd))
    si.system(cmd, *args, **kwargs)


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level)
    #


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    # TODO(gp): Factor this out in dbg.
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")
    _main(parser)
