#!/usr/bin/env python

import argparse
import logging

import helpers.dbg as dbg
import helpers.parser as prsr

_LOG = logging.getLogger(__name__)


# #############################################################################


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
    # Parse files.
    in_file_name, out_file_name = prsr.parse_input_output_args(args)
    _ = in_file_name, out_file_name
    # # Read file.
    # txt = prsr.read_file(in_file_name)
    # # Transform.
    # txt_tmp = "\n".join(txt)
    # cfile = htrace.parse_traceback(txt_tmp)
    # cfile_as_str = htrace.cfile_to_str(cfile)
    # # Write file.
    # prsr.write_file(cfile_as_str.split("\n"), out_file_name)


if __name__ == "__main__":
    _main(_parse())
