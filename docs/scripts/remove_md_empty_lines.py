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

import helpers.dbg as dbg
import helpers.io_ as io_

_LOG = logging.getLogger(__name__)


def _parse():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--input', action='store', type=str, required=True)
    parser.add_argument('--output', action='store', type=str, default=None)
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")
    return parser


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level, use_exec_path=True)
    # Slurp file.
    lines = io_.from_file(args.input)
    lines = [l.rstrip("\n") for l in lines]
    out = []
    # Add some directive for pandoc.
    out.extend([r"""\let\emph\textit""", ""])
    out.extend([r"""\let\uline\underline""", ""])
    out.extend([r"""\let\ul\underline""", ""])
    # During a block to skip.
    in_skip_block = False
    # During a code block.
    in_code_block = False
    for i in range(len(lines)):
        line = lines[i]
        _LOG.debug("%s:line=%s", i, line)
        # Handle comment block.
        # TODO: improve the comment handling, handle also \* *\ and %.
        if line.startswith(r'<!--') or re.search(r'\\\*', line):
            dbg.dassert(not in_skip_block)
            # Start skipping comments.
            in_skip_block = True
        if in_skip_block:
            #dbg.dassert(not in_code_block, msg="line=%s\n%s" % (i, "\n".join(lines[i-2: i+3])))
            if line.startswith(r'-->') or re.search(r'\*\/', line):
                # End skipping comments.
                in_skip_block = False
            # Skip comment.
            _LOG.debug("  -> skip")
            continue
        # Handle code block.
        if re.match('^(\s*)```', line):
            _LOG.debug("  -> code block")
            in_code_block = not in_code_block
            # Add empty line.
            if in_code_block and (i + 1 < len(lines)) and re.match("\s*", lines[i + 1]):
                out.append("\n")
            out.append("    " + line)
            if not in_code_block and (i + 1 < len(lines)) and re.match("\s*", lines[i + 1]):
                out.append("\n")
            continue
        if in_code_block:
            line = line.replace('// ', '# ')
            out.append("    " + line)
            # We don't do any of the other post-processing.
            continue
        # Handle single line comment. We need to do it after the // in code
        # blocks have been handled.
        if line.startswith(r'%%') or line.startswith(r'//'):
            _LOG.debug("  -> skip")
            continue
        # Skip frame.
        if (re.match("\#+ -----", line) or
                re.match("\#+ \#\#\#\#\#", line) or
                re.match("\#+ =====", line) or
                re.match(r"\#+ \/\/\/\/\/", line)):
            _LOG.debug("  -> skip")
            continue
        # Process question.
        if line.startswith("* ") or line.startswith("** ") or line.startswith("*:"):
            # Bold.
            meta = "**"
            # Bold + italic
            #meta = "_**"
            # Underline (not working)
            #meta = "__"
            # Italic.
            #meta = "_"
            if line.startswith("* "):
                to_replace = "* "
                line = line.replace(to_replace, "- " + meta) + meta[::-1]
            elif line.startswith("** "):
                to_replace = "** "
                line = line.replace(to_replace, "- " + meta) + meta[::-1]
            elif line.startswith("*: "):
                to_replace = "*: "
                line = line.replace(to_replace, "- " + meta) + meta[::-1]
            else:
                raise RuntimeError("line=%s" % line)
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
            prev_line_is_verbatim = ((i - 1) > 0) and lines[i - 1].startswith("```")
            next_line_is_verbatim = ((i + 1) < len(lines)) and (
                lines[i + 1].startswith("```"))
            # The next line has a chapter or the start of a new note.
            next_line_is_chapter = ((i + 1) < len(lines)) and (
                    lines[i + 1].startswith("#") or 
                    lines[i + 1].startswith("* "))
            _LOG.debug("  is_empty=%s prev_line_is_verbatim=%s next_line_is_chapter=%s",
                       is_empty, prev_line_is_verbatim, next_line_is_chapter)
            if next_line_is_chapter or prev_line_is_verbatim or next_line_is_verbatim:
                out.append("  " + line)
    # Print result.
    txt = "\n".join(out)
    io_.to_file(args.output, txt)


if __name__ == "__main__":
    _main(_parse())
