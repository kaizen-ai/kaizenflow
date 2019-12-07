#!/usr/bin/env python

"""
Used in vim to prettify only a part of the text.
"""

import logging
import re
import sys
from typing import List

import helpers.io_ as io_
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# #############################################################################

# TODO(gp): Implement
# us -> you
# ours -> yours
# ourselves -> yourself


def _preprocess(txt: str) -> str:
    txt_new: List[str] = []
    for line in txt.split("\n"):
        line = re.sub(r"^\* ", "- STAR", line)
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
    return txt_new_as_str


def _process(txt, file_name):
    #
    # - Pre-process text.
    #
    txt_new_as_str = _preprocess(txt)
    #
    # - Prettify.
    #
    io_.to_file(file_name, txt_new_as_str)
    if True:
        executable = "prettier"
        cmd_opts: List[str] = []
        cmd_opts.append("--parser markdown")
        cmd_opts.append("--prose-wrap always")
        cmd_opts.append("--write")
        cmd_opts.append("--tab-width 4")
        cmd_opts.append("2>&1 >/dev/null")
        cmd_opts_as_str = " ".join(cmd_opts)
        cmd_as_str = " ".join([executable, cmd_opts_as_str, file_name])
        output_tmp = si.system_to_string(cmd_as_str, abort_on_error=True)
        _LOG.debug("output_tmp=%s", output_tmp)
    #
    # - Post-process text.
    #
    txt = io_.from_file(file_name).split("\n")
    txt_new: List[str] = []
    for line in txt:
        line = re.sub(r"^\-   STAR", "*   ", line)
        line = re.sub(r"^\s*\n(\s*\$\$)", r"\\1", line, 0, flags=re.MULTILINE)
        m = re.match(r"(\s*-\s+)(\S)(.*)", line)
        if m:
            line = m.group(1) + m.group(2).upper() + m.group(3)
        txt_new.append(line)
    txt_new_as_str = "\n".join(txt_new).rstrip("\n")
    #
    return txt_new_as_str


def _main():
    # dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    txt = "".join(list(sys.stdin))
    # io_.to_file(file_name, list(sys.stdin))
    file_name = "/tmp/tmp_prettier.txt"
    txt_new_as_str = _process(txt, file_name)
    # Write.
    io_.to_file(file_name, txt_new_as_str)
    print(txt_new_as_str)


if __name__ == "__main__":
    _main()
