#!/usr/bin/env python

"""
Convert a txt file into markdown suitable for pandoc.py

E.g.,
- convert the text in some nice pandoc / latex format
- handle banners around chapters
- handle comments
"""

# TODO(gp):
#  -> convert_txt_to_pandoc.py
#  - Add spaces between lines
#  - Add spaces when format is not correct, e.g.,
#        - kkkk
#        aaaa
#  - Add index counting the indices
#  - DONE: Highlight somehow the question
#      - Indent with extra space the answer, so that Latex adds another level of
#        indentation
#  - Convert // comments in code into #
#  - Fix /* and */

import argparse
import logging
import re
from typing import List

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr

_LOG = logging.getLogger(__name__)


def _transform(lines: List[str]) -> List[str]:
    out : List[str] = []
    # During a block to skip.
    in_skip_block = False
    # During a code block.
    in_code_block = False
    # pylint: disable=consider-using-enumerate
    for i in range(len(lines)):
        line = lines[i]
        _LOG.debug("%s:line=%s", i, line)
        # Handle comment block.
        # TODO: improve the comment handling, handle also \* *\ and %.
        if line.startswith(r"<!--") or re.search(r"\\\*", line):
            dbg.dassert(not in_skip_block)
            # Start skipping comments.
            in_skip_block = True
        if in_skip_block:
            # dbg.dassert(not in_code_block, msg="line=%s\n%s" % (i, "\n".join(lines[i-2: i+3])))
            if line.startswith(r"-->") or re.search(r"\*\/", line):
                # End skipping comments.
                in_skip_block = False
            # Skip comment.
            _LOG.debug("  -> skip")
            continue
        # Handle code block.
        if re.match(r"^(\s*)```", line):
            _LOG.debug("  -> code block")
            in_code_block = not in_code_block
            # Add empty line.
            if (
                in_code_block
                and (i + 1 < len(lines))
                and re.match(r"\s*", lines[i + 1])
            ):
                out.append("\n")
            out.append("    " + line)
            if (
                not in_code_block
                and (i + 1 < len(lines))
                and re.match(r"\s*", lines[i + 1])
            ):
                out.append("\n")
            continue
        if in_code_block:
            line = line.replace("// ", "# ")
            out.append("    " + line)
            # We don't do any of the other post-processing.
            continue
        # Handle single line comment. We need to do it after the // in code
        # blocks have been handled.
        if line.startswith(r"%%") or line.startswith(r"//"):
            _LOG.debug("  -> skip")
            continue
        # Skip frame.
        if (
            re.match(r"\#+ -----", line)
            or re.match(r"\#+ \#\#\#\#\#", line)
            or re.match(r"\#+ =====", line)
            or re.match(r"\#+ \/\/\/\/\/", line)
        ):
            _LOG.debug("  -> skip")
            continue
        # Process question.
        # num_tab_spaces = 4
        num_tab_spaces = 2
        space = " " * (num_tab_spaces - 1)
        if (
            line.startswith("*" + space)
            or line.startswith("**" + space)
            or line.startswith("*:" + space)
        ):
            # Bold.
            meta = "**"
            # Bold + italic
            # meta = "_**"
            # Underline (not working)
            # meta = "__"
            # Italic.
            # meta = "_"
            if line.startswith("*" + space):
                to_replace = "*" + space
            elif line.startswith("**" + space):
                to_replace = "**" + space
            elif line.startswith("*:" + space):
                to_replace = "*: "
            else:
                raise RuntimeError("line=%s" % line)
            line = line.replace(to_replace, "- " + meta) + meta[::-1]
            out.append(line)
            continue
        # Handle empty lines in the questions and answers.
        is_empty = line.rstrip(" ").lstrip(" ") == ""
        if not is_empty:
            if line.startswith("#"):
                # It's a chapter.
                out.append(line)
            else:
                # It's a line in an answer.
                out.append("  " + line)
        else:
            # Empty line.
            prev_line_is_verbatim = ((i - 1) > 0) and lines[i - 1].startswith(
                "```"
            )
            next_line_is_verbatim = ((i + 1) < len(lines)) and (
                lines[i + 1].startswith("```")
            )
            # The next line has a chapter or the start of a new note.
            next_line_is_chapter = ((i + 1) < len(lines)) and (
                lines[i + 1].startswith("#") or lines[i + 1].startswith("* ")
            )
            _LOG.debug(
                "  is_empty=%s prev_line_is_verbatim=%s next_line_is_chapter=%s",
                is_empty,
                prev_line_is_verbatim,
                next_line_is_chapter,
            )
            if (
                next_line_is_chapter
                or prev_line_is_verbatim
                or next_line_is_verbatim
            ):
                out.append("  " + line)
    return out


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--input", action="store", type=str, required=True)
    parser.add_argument("--output", action="store", type=str, default=None)
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Slurp file.
    lines = io_.from_file(args.input)
    lines = [l.rstrip("\n") for l in lines]
    out : List[str] = []
    # Add some directive for pandoc.
    out.extend([r"""\let\emph\textit""", ""])
    out.extend([r"""\let\uline\underline""", ""])
    out.extend([r"""\let\ul\underline""", ""])
    #
    out_tmp = _transform(lines)
    out.extend(out_tmp)
    # Print result.
    txt = "\n".join(out)
    io_.to_file(args.output, txt)


if __name__ == "__main__":
    _main(_parse())
