#!/usr/bin/env python

import argparse
import logging
import re

import helpers.dbg as dbg
import helpers.parser as prsr

_LOG = logging.getLogger(__name__)


# #############################################################################


import re


def _remove_non_printable(txt: str) -> str:
    # From https://stackoverflow.com/questions/14693701
    # 7-bit and 8-bit C1 ANSI sequences
    ansi_escape = re.compile(
        r"""
        \x1B  # ESC
        (?:   # 7-bit C1 Fe (except CSI)
            [@-Z\\-_]
        |     # or [ for CSI, followed by a control sequence
            \[
            [0-?]*  # Parameter bytes
            [ -/]*  # Intermediate bytes
            [@-~]   # Final byte
        )
    """,
        re.VERBOSE,
    )
    txt = ansi_escape.sub("", txt)
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
    in_file_name, out_file_name = prsr.parse_input_output_args(
        args, clear_screen=False
    )
    txt = prsr.read_file(in_file_name)
    txt_tmp = "\n".join(txt)
    txt_tmp = _remove_non_printable(txt_tmp)
    txt_tmp = txt_tmp.split("\n")
    prsr.write_file(txt_tmp, out_file_name)


if __name__ == "__main__":
    _main(_parse())
