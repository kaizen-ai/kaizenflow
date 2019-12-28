#!/usr/bin/env python

"""
Remove pytest artifacts.
"""

import argparse
import json
import logging
import os

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.printing as prnt
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--preview", action="store_true")
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=False)
    #
    si.pytest_clean_artifacts(".", preview = args.preview)


if __name__ == "__main__":
    _main(_parse())
