#!/usr/bin/env python

"""
Transform the output of `diff -r --brief dir1 dir2` into a script using vimdiff.

> diff -r --brief /Users/saggese/src/commodity_research2/amp /Users/saggese/src/commodity_research3/amp | tee diff.txt
> diff_to_vimdiff.py -i diff.txt

> diff_to_vimdiff.py -i diff.txt -o vim_diff.sh
"""

import argparse
import logging
import re

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr

_LOG = logging.getLogger(__name__)


def _parse_diff_output(input_file, output_file):
    # Read.
    dbg.dassert_exists(input_file)
    _LOG.info("Reading '%s'", input_file)
    txt = io_.from_file(input_file, split=False)
    txt = txt.split("\n")
    # Process.
    out = []
    for line in txt:
        _LOG.debug("# line='%s'", line)
        out.append(line)
        if line.startswith("Only in "):
            # Only in /data/gp_wd/src/deploy_particle1/: cfile
            m = re.match(r"^Only in (\S+): (\S+)$", line)
            dbg.dassert(m, "Invalid line='%s'", line)
        elif line.startswith("Files "):
            # Files
            #   /data/gp_wd/src/deploy_particle1/compustat/fiscal_calendar.py and
            #   /data/gp_wd/src/particle1/compustat/fiscal_calendar.py differ
            m = re.match(r"^Files (\S+) and (\S+) differ$", line)
            dbg.dassert(m, "Invalid line='%s'", line)
            out_line = "vimdiff %s %s" % (m.group(1), m.group(2))
        else:
            dbg.dfatal("Invalid line='%s'", line)
        _LOG.debug("# line='%s'", line)
        _LOG.debug("    -> out='%s'", out_line)
        out.append(out_line)
    #
    out = "\n".join(out)
    if output_file == "-":
        print(out)
    else:
        _LOG.info("Writing '%s'", output_file)
        io_.to_file(output_file, out)


# ##############################################################################


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("positional", nargs="*", help="...")
    parser.add_argument(
        "-i", "--input_file", action="store", required=True, help="Input file"
    )
    parser.add_argument(
        "-o",
        "--output_file",
        action="store",
        help="Output file. Use `-` for stdout",
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level, use_exec_path=True)
    #
    _parse_diff_output(args.input_file, args.output_file)


if __name__ == "__main__":
    _main(_parse())
