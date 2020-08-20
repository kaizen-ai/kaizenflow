#!/usr/bin/env python
r"""Wrapper for lint_txt.py text

> p1_lint_md.py sample_file1.md sample_file2.md
"""
import argparse
import logging
import os
from typing import List

import dev_scripts.linter.base as lntr
import helpers.dbg as dbg
import helpers.git as git
import helpers.parser as prsr
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


# #############################################################################


def _check_readme_is_capitalized(file_name: str) -> str:
    """Check if all readme markdown files are named README.md."""
    msg = ""
    basename = os.path.basename(file_name)

    if basename.lower() == "readme.md" and basename != "README.md":
        msg = f"{file_name}:1: All README files should be named README.md"
    return msg


# #############################################################################


class _LintMarkdown(lntr.Action):
    def __init__(self) -> None:
        executable = "prettier"
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        check: bool = si.check_exec(self._executable)
        return check

    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        # Applicable only to txt and md files.
        ext = os.path.splitext(file_name)[1]
        output: List[str] = []
        if ext not in (".txt", ".md"):
            _LOG.debug("Skipping file_name='%s' because ext='%s'", file_name, ext)
            return output
        # Run lint_txt.py.
        executable = "lint_txt.py"
        exec_path = git.find_file_in_git_tree(executable)
        dbg.dassert_exists(exec_path)
        #
        cmd = []
        cmd.append(exec_path)
        cmd.append("-i %s" % file_name)
        cmd.append("--in_place")
        cmd_as_str = " ".join(cmd)
        _, output = lntr.tee(cmd_as_str, executable, abort_on_error=True)
        # Check file name.
        msg = _check_readme_is_capitalized(file_name)
        if msg:
            output.append(msg)
        # Remove cruft.
        output = [
            line
            for line in output
            if "Saving log to file" not in line
            # Remove reset character from output so pre-commit hook doesn't fail.
            and line != "\x1b[0m"
        ]

        return output


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "files", nargs="+", action="store", type=str, help="Files to process",
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level)
    action = _LintMarkdown()
    lntr.run_action(action, args.files)


if __name__ == "__main__":
    _main(_parse())
