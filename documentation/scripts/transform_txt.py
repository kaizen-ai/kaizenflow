#!/usr/bin/env python

"""
Perform one of several transformations on a txt file.

- The input or output can be filename or stdin (represented by '-')
- If output file is not specified then we assume that the output file is the
  same as the input

- The possible transformations are:
    1) Create table of context from the current file, with 1 level
        > transform_txt.py -a toc -i % -l 1

    2) Format the current file with 3 levels
        :!transform_txt.py -a format -i % --max_lev 3
        > transform_txt.py -a format -i notes/ABC.txt --max_lev 3

        - In vim
        :!transform_txt.py -a format -i % --max_lev 3
        :%!transform_txt.py -a format -i - --max_lev 3

    3) Increase level
        :!transform_txt.py -a increase -i %
        :%!transform_txt.py -a increase -i -
"""

# TODO(gp):
#  - Compute index number
#  - Add unit tests
#  - Make functions private

import argparse
import logging
import os
import re
import sys

import helpers.dbg as dbg
import helpers.parser as prsr

_LOG = logging.getLogger(__name__)


# TODO(gp): Factor this out.
def _read_file(file_name):
    """
    Read file or stdin, returning an array of lines.
    """
    if file_name == "-":
        f = sys.stdin
    else:
        f = open(file_name, "r")
    # Read.
    txt = []
    for line in f:
        line = line.rstrip("\n")
        txt.append(line)
    f.close()
    return txt


def _write_file(file_name, txt):
    if file_name == "-":
        print("\n".join(txt))
    else:
        with open(file_name, "w") as f:
            f.write("\n".join(txt))


# #############################################################################


def skip_comments(line, skip_block):
    skip_this_line = False
    # Handle comment block.
    if line.startswith("<!--"):
        # Start skipping comments.
        skip_block = True
        skip_this_line = True
    if skip_block:
        skip_this_line = True
        if line.startswith("-->"):
            # End skipping comments.
            skip_block = False
        else:
            # Skip comment.
            _LOG.debug("  -> skip")
    else:
        # Handle single line comment.
        if line.startswith("%%"):
            _LOG.debug("  -> skip")
            skip_this_line = True
    return skip_this_line, skip_block


def table_of_content(file_name, max_lev):
    skip_block = False
    txt = _read_file(file_name)
    for l in txt:
        # Skip comments.
        skip_this_line, skip_block = skip_comments(l, skip_block)
        if False and skip_this_line:
            continue
        #
        for i in range(1, max_lev + 1):
            if l.startswith("#" * i + " "):
                if (
                    ("#########" not in l)
                    and ("///////" not in l)
                    and ("-------" not in l)
                    and ("======" not in l)
                ):
                    if i == 1:
                        print()
                    print("%s%s" % ("    " * (i - 1), l))
                break


def format_text(in_file_name, out_file_name, max_lev):
    txt = _read_file(in_file_name)
    #
    for line in txt:
        m = re.search("max_level=(\d+)", line)
        if m:
            max_lev = int(m.group(1))
            _LOG.warning("Inferred max_level=%s", max_lev)
            break
    dbg.dassert_lte(1, max_lev)
    # Remove all headings.
    txt_tmp = []
    for l in txt:
        # Keep the comments.
        if not (
            re.match("#+ ####+", l)
            or re.match("#+ /////+", l)
            or re.match("#+ ------+", l)
            or re.match("#+ ======+", l)
        ):
            txt_tmp.append(l)
    txt = txt_tmp[:]
    # Add proper heading of the correct length.
    txt_tmp = []
    for l in txt:
        # Keep comments.
        found = False
        for i in range(1, max_lev + 1):
            if l.startswith("#" * i + " "):
                row = "#" * i + " " + "#" * (79 - 1 - i)
                txt_tmp.append(row)
                txt_tmp.append(l)
                txt_tmp.append(row)
                found = True
        if not found:
            txt_tmp.append(l)
    # TODO(gp): Remove all empty lines after a heading.
    # TODO(gp): Format title (first line capital and then small).
    _write_file(out_file_name, txt_tmp)


def increase_chapter(in_file_name, out_file_name):
    """
    Increase the level of chapters by one for text in stdin.
    """
    skip_block = False
    txt = _read_file(in_file_name)
    #
    txt_tmp = []
    for line in txt:
        skip_this_line, skip_block = skip_comments(line, skip_block)
        if skip_this_line:
            continue
        #
        line = line.rstrip(r"\n")
        for i in range(1, 5):
            if line.startswith("#" * i + " "):
                line = line.replace("#" * i + " ", "#" * (i + 1) + " ")
                break
        txt_tmp.append(line)
    #
    _write_file(out_file_name, txt_tmp)


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-a", "--action", choices=["toc", "format", "increase"], required=True
    )
    parser.add_argument("-i", "--in_file_name", required=True)
    parser.add_argument("-o", "--out_file_name", required=False, default=None)
    parser.add_argument("-l", "--max_lev", default=5)
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    print("cmd line: %s" % dbg.get_command_line())
    _LOG.setLevel(args.log_level)
    #
    cmd = args.action
    max_lev = int(args.max_lev)
    in_file_name = args.in_file_name
    out_file_name = args.out_file_name
    if out_file_name is None:
        out_file_name = in_file_name
    # Print label.
    if in_file_name != "-":
        os.system("clear")
        print("in_file_name=", in_file_name)
        print("out_file_name=", out_file_name)
        print("max_lev=", max_lev)
    #
    if cmd == "toc":
        table_of_content(in_file_name, max_lev)
    elif cmd == "format":
        format_text(in_file_name, out_file_name, max_lev)
    elif cmd == "increase":
        increase_chapter(in_file_name, out_file_name)
    else:
        assert 0, "Invalid cmd='%s'" % cmd


if __name__ == "__main__":
    _main(_parse())
