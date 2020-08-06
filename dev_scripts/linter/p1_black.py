#!/usr/bin/env python
r"""Wrapper for black

> p1_black.py sample_file1.py sample_file2.py
"""
import argparse
import logging
from typing import List

import helpers.dbg as dbg
import helpers.parser as prsr
import helpers.system_interaction as si
import dev_scripts.linter.base as lntr
import dev_scripts.linter.utils as utils

_LOG = logging.getLogger(__name__)


# #############################################################################


class _Black(lntr.Action):
    """Apply black code formatter."""

    def __init__(self) -> None:
        executable = "black"
        super().__init__(executable)

    def check_if_possible(self) -> bool:
        check: bool = si.check_exec(self._executable)
        return check

    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        _ = pedantic
        # Applicable to only python file.
        if not utils.is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []
        #
        opts = "--line-length 82"
        cmd = self._executable + " %s %s" % (opts, file_name)
        _, output = lntr.tee(cmd, self._executable, abort_on_error=False)
        # Remove the lines:
        # - reformatted core/test/test_core.py.
        # - 1 file reformatted.
        # - All done!
        # - 1 file left unchanged.
        to_remove = ["All done!", "file left unchanged", "reformatted"]
        output = [
            line for line in output if all(word not in line for word in to_remove)
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
    action = _Black()
    lntr.run_action(action, args.files)


if __name__ == "__main__":
    _main(_parse())
