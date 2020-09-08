#!/usr/bin/env python
r"""Wrapper for mypy

> p1_mypy.py sample_file1.py sample_file2.py

> p1_mypy.py sample_file1.py -v DEBUG
"""
import argparse
import functools
import logging
from typing import List

import dev_scripts.linter2.base as lntr
import dev_scripts.linter2.utils as utils
import helpers.dbg as dbg
import helpers.git as git
import helpers.parser as prsr
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


# #############################################################################


class _Mypy(lntr.Action):
    def __init__(self) -> None:
        executable = "mypy"
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        check: bool = si.check_exec(self._executable)
        return check

    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        _ = pedantic
        # Applicable to only python files, that are not paired with notebooks.
        if not utils.is_py_file(file_name) or utils.is_paired_jupytext_file(
            file_name
        ):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []
        # TODO(gp): Convert all these idioms into arrays and joins.
        cmd = (
            f"{self._executable} --config-file {self._config_path()} {file_name}"
        )
        _, output = lntr.tee(
            cmd,
            self._executable,
            # Mypy returns -1 if there are errors.
            abort_on_error=False,
        )
        # Remove some errors.
        output_tmp: List[str] = []
        for line in output:
            if (
                line.startswith("Success:")
                or
                # Found 2 errors in 1 file (checked 1 source file)
                line.startswith("Found ")
                or
                # Note: See https://mypy.readthedocs.io.
                "note: See https" in line
            ):
                continue
            output_tmp.append(line)
        output = output_tmp
        return output

    @staticmethod
    @functools.lru_cache()
    def _config_path() -> str:
        """Return path of mypy.ini file.

        :raise: RuntimeError if mypy.ini is not found
        """
        path: str = git.find_file_in_git_tree("mypy.ini", super_module=False)
        return path


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
    action = _Mypy()
    lntr.run_action(action, args.files)


if __name__ == "__main__":
    _main(_parse())
