#!/usr/bin/env python
"""Wrapper for docformatter.

> p1_doc_formatter.py sample_file1.py sample_file2.py
"""
import argparse
import logging
import re
from typing import List

import helpers.dbg as dbg
import helpers.parser as prsr
import helpers.system_interaction as si
import dev_scripts.linter.base as lntr
import dev_scripts.linter.utils as utils

_LOG = logging.getLogger(__name__)


class _DocFormatter(lntr.Action):
    """Format docstrings to follow a subset of the PEP 257 conventions It
    relies on:

    - docformatter
    """

    def __init__(self) -> None:
        executable = "docformatter"
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        """Check if action can run."""
        check: bool = si.check_exec(self._executable)
        return check

    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        """Run docformatter on file.

        :param file_name: str:
        :param pedantic: int:
        """
        _ = pedantic
        # Applicable to only python file.
        if not utils.is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []

        cmd = f"{self._executable} --in-place {file_name}"
        output: List[str]
        _, output = lntr.tee(cmd, self._executable, abort_on_error=False)
        return output


# #############################################################################


class _Pydocstyle(lntr.Action):
    """"""

    def __init__(self) -> None:
        executable = "pydocstyle"
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        """Check if action can run."""
        check: bool = si.check_exec(self._executable)
        return check

    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        """Run pydocstyle on file.

        :param file_name: str:
        :param pedantic: int:
        """
        # Applicable to only python file.
        if not utils.is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []
        ignore = []
        # Error codes: http://www.pydocstyle.org/en/2.1.1/error_codes.html.
        if pedantic < 2:
            # TODO(gp): Review all of these.
            ignore.extend(
                [
                    # D105: Missing docstring in magic method.
                    "D105",
                    # D200: One-line docstring should fit on one line with quotes.
                    "D200",
                    # D202: No blank lines allowed after function docstring.
                    "D202",
                    # D213: Multi-line docstring summary should start at the second line.
                    "D213",
                    # D209: Multi-line docstring closing quotes should be on a separate line.
                    "D209",
                    # D203: 1 blank line required before class docstring (found 0)
                    "D203",
                    # D205: 1 blank line required between summary line and description.
                    "D205",
                    # D400: First line should end with a period (not ':')
                    "D400",
                    # D402: First line should not be the function's "signature"
                    "D402",
                    # D407: Missing dashed underline after section.
                    "D407",
                    # D413: Missing dashed underline after section.
                    "D413",
                    ## D415: First line should end with a period, question mark,
                    ## or exclamation point.
                    "D415",
                ]
            )
        if pedantic < 1:
            # Disable some lints that are hard to respect.
            ignore.extend(
                [
                    # D100: Missing docstring in public module.
                    "D100",
                    # D101: Missing docstring in public class.
                    "D101",
                    # D102: Missing docstring in public method.
                    "D102",
                    # D103: Missing docstring in public function.
                    "D103",
                    # D104: Missing docstring in public package.
                    "D104",
                    # D107: Missing docstring in __init__
                    "D107",
                ]
            )
        cmd_opts = ""
        if ignore:
            cmd_opts += "--ignore " + ",".join(ignore)
        #
        cmd = []
        cmd.append(self._executable)
        cmd.append(cmd_opts)
        cmd.append(file_name)
        cmd_as_str = " ".join(cmd)
        ## We don't abort on error on pydocstyle, since it returns error if there
        ## is any violation.
        _, file_lines_as_str = si.system_to_string(
            cmd_as_str, abort_on_error=False
        )
        ## Process lint_log transforming:
        ##   linter_v2.py:1 at module level:
        ##       D400: First line should end with a period (not ':')
        ## into:
        ##   linter_v2.py:1: at module level: D400: First line should end with a
        ##   period (not ':')
        #
        output: List[str] = []
        #
        file_lines = file_lines_as_str.split("\n")
        lines = ["", ""]
        for cnt, line in enumerate(file_lines):
            line = line.rstrip("\n")
            # _log.debug("line=%s", line)
            if cnt % 2 == 0:
                regex = r"(\s(at|in)\s)"
                subst = r":\1"
                line = re.sub(regex, subst, line)
            else:
                line = line.lstrip()
            # _log.debug("-> line=%s", line)
            lines[cnt % 2] = line
            if cnt % 2 == 1:
                line = "".join(lines)
                output.append(line)
        return output


# #############################################################################


# TODO(gp): Fix this.
# Not installable through conda.
class _Pyment(lntr.Action):
    """"""

    def __init__(self) -> None:
        executable = "pyment"
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        """Check if action can run."""
        check: bool = si.check_exec(self._executable)
        return check

    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        """Run pyment on file.

        :param file_name: str:
        :param pedantic: int:
        """
        _ = pedantic
        # Applicable to only python file.
        if not utils.is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []
        opts = "-w --first-line False -o reST"
        cmd = self._executable + " %s %s" % (opts, file_name)
        output: List[str]
        _, output = lntr.tee(cmd, self._executable, abort_on_error=False)
        return output


# #############################################################################


class _P1DocFormatterAction:
    """An action that groups all docstring linter actions."""

    @staticmethod
    def _execute(file_name: str, pedantic: int) -> List[str]:
        """Execute all docstring formatting actions in an interface similar to
        other actions, so we can use `lntr.run_action`

        :param file_name: str:
        :param pedantic: int:
        """
        actions: List[lntr.Action] = [
            _DocFormatter,
            _Pydocstyle,
            ## Pyment's config doesn't play well with other formatters,
            ## possible problems I see so far:
            ##  1. it adds a lot of trailing white spaces.
            ##  2. it reads already existing arguments in the docstring (very weird)
            ##  3. it moves start of multi-line doc strings to 2nd line, which conflicts
            ##     with docformatter (possibly can be configured)
            # _Pyment,
        ]
        output: List[str] = []
        for action in actions:
            output.extend(
                action()._execute(  # pylint: disable=protected-access; temporary
                    file_name, pedantic
                )
            )
        return output


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    """"""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "files", nargs="+", action="store", type=str, help="Files to process",
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    """Run _P1DocFormatterAction.

    :param parser: argparse.ArgumentParser:
    """
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level)
    action = _P1DocFormatterAction()
    lntr.run_action(action, args.files)


if __name__ == "__main__":
    _main(_parse())
