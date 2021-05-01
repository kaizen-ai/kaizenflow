#!/usr/bin/env python

"""
Parse a file with a traceback and generates a cfile

> pytest -x helpers/test/test_traceback.py --dbg | tee log.txt
> dev_scripts/traceback_to_cfile.py -i log.txt
"""

import argparse
import logging

import helpers.dbg as dbg
import helpers.parser as prsr
import helpers.printing as hprint
import helpers.traceback_helper as htrace

_LOG = logging.getLogger(__name__)

# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    prsr.add_input_output_args(parser, out_default="cfile")
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=False)
    # Parse files.
    in_file_name, out_file_name = prsr.parse_input_output_args(args, clear_screen=True)
    # Read file.
    txt = prsr.read_file(in_file_name)
    # Transform.
    txt_tmp = "\n".join(txt)
    cfile, traceback = htrace.parse_traceback(txt_tmp)
    print(hprint.frame("traceback") + "\n" + traceback)
    cfile_as_str = htrace.cfile_to_str(cfile)
    print(hprint.frame("cfile") + "\n" + cfile_as_str)
    # Write file.
    prsr.write_file(cfile_as_str.split("\n"), out_file_name)


if __name__ == "__main__":
    _main(_parse())