#!/usr/bin/env python

import argparse
import logging
import os
import re
import sys

import helpers.dbg as dbg
import helpers.parser as prsr

_LOG = logging.getLogger(__name__)


# #############################################################################


import string

def _remove_non_printable(txt: str) -> str:
    #print(txt)
    #txt = ''.join(c for c in txt if c not in string.printable)
    #txt = ''.join(filter(lambda x: x in string.printable, txt))
    # ^[[41m
    txt =
    print(txt)
    return txt


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    prsr.add_input_output_args(parser)
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    print("cmd line: %s" % dbg.get_command_line())
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    #
    in_file_name, out_file_name = prsr.parse_input_output_args(args, clear_screen=False)
    txt = prsr.read_file(in_file_name)
    txt_tmp = "\n".join(txt)
    txt_tmp = _remove_non_printable(txt_tmp)
    prsr.write_file(txt_tmp, out_file_name)


if __name__ == "__main__":
    _main(_parse())