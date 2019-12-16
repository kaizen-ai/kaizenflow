#!/usr/bin/env python

"""
Lint md files.
> lint_txt.py -i foo.md -o bar.md

It can be used in vim to prettify a part of the text using stdin / stdout.
:%!lint_txt.py
"""

# TODO(gp): -> lint_md.py

import argparse
import logging
import re
import sys
import tempfile
from typing import List

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# #############################################################################

# TODO(gp): Implement these replacements for `.txt`.
#  us -> you
#  ours -> yours
#  ourselves -> yourself

def _preprocess(txt: str) -> str:
    _LOG.debug("txt=%s", txt)
    # Remove some artifacts when copying from gdoc.
    txt = re.sub(r"’", "'", txt)
    txt = re.sub(r"“", '"', txt)
    txt = re.sub(r"”", '"', txt)
    txt = re.sub(r"…", "...", txt)
    txt_new: List[str] = []
    for line in txt.split("\n"):
        # Skip frames.
        if re.match(r"#+ [#\/\-\=]{6,}", line):
            continue
        line = re.sub(r"^\s*\*\s+", "- STAR", line)
        # Transform:
        # $$E_{in} = \frac{1}{N} \sum_i e(h(\vx_i), y_i)$$
        #
        # $$E_{in}(\vw) = \frac{1}{N} \sum_i \big(
        # -y_i \log(\Pr(h(\vx) = 1|\vx)) - (1 - y_i) \log(1 - \Pr(h(\vx)=1|\vx))
        # \big)$$
        #
        # $$
        if re.search(r"^\s*\$\$\s*$", line):
            txt_new.append(line)
            continue
        # $$ ... $$
        m = re.search(r"^(\s*)(\$\$)(.+)(\$\$)\s*$", line)
        if m:
            for i in range(3):
                txt_new.append(m.group(1) + m.group(2 + i))
            continue
        # ... $$
        m = re.search(r"^(\s*)(\$\$)(.+)$", line)
        if m:
            for i in range(2):
                txt_new.append(m.group(1) + m.group(2 + i))
            continue
        # $$ ...
        m = re.search(r"^(\s*)(.*)(\$\$)$", line)
        if m:
            for i in range(2):
                txt_new.append(m.group(1) + m.group(2 + i))
            continue
        txt_new.append(line)
    txt_new_as_str = "\n".join(txt_new)
    _LOG.debug("txt_new_as_str=%s", txt_new_as_str)
    return txt_new_as_str


def _prettier(txt: str) -> str:
    _LOG.debug("txt=%s", txt)
    #
    tmp_file_name = tempfile.NamedTemporaryFile().name
    # tmp_file_name = "/tmp/tmp_prettier.txt"
    io_.to_file(tmp_file_name, txt)
    #
    executable = "prettier"
    cmd_opts: List[str] = []
    cmd_opts.append("--parser markdown")
    cmd_opts.append("--prose-wrap always")
    cmd_opts.append("--write")
    tab_width = 4
    cmd_opts.append("--tab-width %s" % tab_width)
    cmd_opts.append("2>&1 >/dev/null")
    cmd_opts_as_str = " ".join(cmd_opts)
    cmd_as_str = " ".join([executable, cmd_opts_as_str, tmp_file_name])
    output_tmp = si.system_to_string(cmd_as_str, abort_on_error=True)
    _LOG.debug("output_tmp=%s", output_tmp)
    #
    txt = io_.from_file(tmp_file_name)
    return txt


def _postprocess(txt: str) -> str:
    _LOG.debug("txt=%s", txt)
    # Remove empty lines before higher level bullets, but not chapters.
    txt = re.sub(r"^\s*\n(\s+-\s+.*)$", r"\1", txt, 0, flags=re.MULTILINE)
    txt_new: List[str] = []
    for i, line in enumerate(txt.split("\n")):
        # Undo the transformation `* -> STAR`.
        line = re.sub(r"^\-(\s*)STAR", r"*\1", line, 0)
        # Remove empty lines.
        line = re.sub(r"^\s*\n(\s*\$\$)", r"\1", line, 0, flags=re.MULTILINE)
        # Upper case for `- hello`.
        m = re.match(r"(\s*-\s+)(\S)(.*)", line)
        if m:
            line = m.group(1) + m.group(2).upper() + m.group(3)
        # Upper case for `\d) hello`.
        m = re.match(r"(\s*\d+[\)\.]\s+)(\S)(.*)", line)
        if m:
            line = m.group(1) + m.group(2).upper() + m.group(3)
        #
        txt_new.append(line)
    txt_new_as_str = "\n".join(txt_new).rstrip("\n")
    return txt_new_as_str

import helpers.git as git
import os

def _refresh_toc(txt: str) -> str:
    _LOG.debug("txt=%s", txt)
    # Check whether there is a TOC otherwise add it.
    txt_as_arr = txt.split("\n")
    if txt_as_arr[0] != "<!--ts-->":
        _LOG.warning(
            "No tags for table of content in md file: adding it"
        )
        txt = "<!--ts-->\n<!--te-->\n" + txt
    # Write file.
    tmp_file_name = tempfile.NamedTemporaryFile().name
    io_.to_file(tmp_file_name, txt)
    # Process TOC.
    amp_path = git.get_amp_abs_path()
    cmd: List[str] = []
    gh_md_toc = os.path.join(amp_path, "documentation/scripts/gh-md-toc")
    dbg.dassert_exists(gh_md_toc)
    cmd.append(gh_md_toc)
    cmd.append("--insert %s" % tmp_file_name)
    cmd_as_str = " ".join(cmd)
    si.system(cmd_as_str, abort_on_error=False, suppress_output=True)
    # Read file.
    txt = io_.from_file(tmp_file_name)
    return txt


def _process(txt: str, in_file_name: str) -> str:
    # Pre-process text.
    txt = _preprocess(txt)
    # Prettify.
    txt = _prettier(txt)
    # Post-process text.
    txt = _postprocess(txt)
    # Refresh table of content.
    is_md_file = in_file_name.endswith(".md")
    if is_md_file:
        txt = _refresh_toc(txt)
    return txt


# ##############################################################################


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-i",
        "--infile",
        nargs="?",
        type=argparse.FileType("r"),
        default=sys.stdin,
    )
    parser.add_argument(
        "-o",
        "--outfile",
        nargs="?",
        type=argparse.FileType("w"),
        default=sys.stdout,
    )
    parser.add_argument(
        "--in_place", action="store_true",
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(args):
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Read input.
    in_file_name = args.infile.name
    _LOG.debug("in_file_name=%s", in_file_name)
    if in_file_name != "<stdin>":
        dbg.dassert(
            in_file_name.endswith(".txt") or in_file_name.endswith(".md"),
            "Invalid extension for file name '%s'", in_file_name)
    txt = args.infile.read()
    # Process.
    txt = _process(txt, in_file_name)
    # Write output.
    if args.in_place:
        dbg.dassert_ne(in_file_name, "<stdin>")
        io_.to_file(in_file_name, txt)
    else:
        args.outfile.write(txt)


if __name__ == "__main__":
    parser_ = _parser()
    args_ = parser_.parse_args()
    _main(args_)
