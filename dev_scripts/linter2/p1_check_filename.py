#!/usr/bin/env python
r"""Check if filenames are correct according to our standard.

> p1_check_filename.py sample_file1.py sample_file2.py"""
import argparse
import logging
import os
import re
from typing import Callable, List

import dev_scripts.linter2.base as lntr
import dev_scripts.linter2.utils as utils
import helpers.dbg as dbg
import helpers.parser as prsr

_LOG = logging.getLogger(__name__)


def _check_notebook_dir(file_name: str) -> str:
    """Check if that notebooks are under `notebooks` dir."""
    msg = ""
    if utils.is_ipynb_file(file_name):
        subdir_names = file_name.split("/")
        if "notebooks" not in subdir_names:
            msg = (
                "%s:1: each notebook should be under a 'notebooks' "
                "directory to not confuse pytest" % file_name
            )
    return msg


def _check_test_file_dir(file_name: str) -> str:
    """Check if test files are under `test` dir."""
    msg = ""
    # TODO(gp): A little annoying that we use "notebooks" and "test".
    if utils.is_py_file(file_name) and os.path.basename(file_name).startswith(
        "test_"
    ):
        if not utils.is_under_test_dir(file_name):
            msg = (
                "%s:1: test files should be under 'test' directory to "
                "be discovered by pytest" % file_name
            )
    return msg


def _check_notebook_filename(file_name: str) -> str:
    r"""Check notebook filenames start with `Master_` or match: `\S+Task\d+_...`"""
    msg = ""

    basename = os.path.basename(file_name)
    if utils.is_ipynb_file(file_name) and not any(
        [basename.startswith("Master_"), re.match(r"^\S+Task\d+_", basename)]
    ):
        msg = (
            f"{file_name}:1: "
            r"All notebook filenames start with `Master_` or match: `\S+Task\d+_...`"
        )
    return msg


class _P1CheckFilename(lntr.Action):
    """Detect class methods that are in the wrong order."""

    def check_if_possible(self) -> bool:
        return True

    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        """Perform various checks based on the path of a file:

        - check that notebook files are under a `notebooks` dir
        - check that test files are under `test` dir
        """
        _ = pedantic
        if not utils.is_py_file(file_name) and not utils.is_ipynb_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []

        FilePathCheck = Callable[[str], str]
        FILE_PATH_CHECKS: List[FilePathCheck] = [
            _check_notebook_dir,
            _check_test_file_dir,
            _check_notebook_filename,
        ]

        output: List[str] = []
        for func in FILE_PATH_CHECKS:
            msg = func(file_name)
            if msg:
                _LOG.warning(msg)
                output.append(msg)

        return output


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-e",
        "--enforce",
        action="store_true",
        default=False,
        help="Enforce method order",
    )
    parser.add_argument(
        "files", nargs="+", action="store", type=str, help="Files to process",
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level)
    action = _P1CheckFilename()
    lntr.run_action(action, args.files)


if __name__ == "__main__":
    _main(_parse())
