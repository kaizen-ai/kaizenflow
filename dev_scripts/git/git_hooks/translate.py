#!/usr/bin/env python

import argparse
import logging

import helpers.dbg as dbg
import helpers.parser as prsr
import dev_scripts.git.git_hooks.utils as ghutils

_LOG = logging.getLogger(__name__)

# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--text", action="store", type=str, required=True)
    parser.add_argument("--step", action="store", type=int, required=True)
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    transformed_txt = ghutils.caesar(args.text, args.step)
    print(transformed_txt)


if __name__ == "__main__":
    _main(_parse())
