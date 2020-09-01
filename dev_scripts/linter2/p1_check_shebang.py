import os
import logging
import argparse

import dev_scripts.linter2.utils as utils
import dev_scripts.linter2.base as lntr
import helpers.parser as prsr
import helpers.io_ as io_
import helpers.dbg as dbg

from typing import List


_LOG = logging.getLogger(__name__)


def _check_shebang(file_name: str, lines: List[str]) -> str:
    """Return warning if:

    - a python executable has no shebang
    - a python file has a shebang and isn't executable

    Note: the function ignores __init__.py files  & test code.
    """
    msg = ""
    if os.path.basename(file_name) == "__init__.py" or utils.is_test_code(
            file_name
    ):
        return msg

    shebang = "#!/usr/bin/env python"
    has_shebang = lines[0] == shebang
    is_executable = os.access(file_name, os.X_OK)

    if is_executable and not has_shebang:
        msg = f"{file_name}:1: any executable needs to start with a shebang '{shebang}'"
    elif not is_executable and has_shebang:
        msg = f"{file_name}:1: a non-executable can't start with a shebang."

    return msg


class _P1CheckShebang(lntr.Action):
    """Check if executables start with a shebang"""

    def check_if_possible(self) -> bool:
        return True

    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        _ = pedantic

        if not utils.is_ipynb_file(file_name) and not utils.is_ipynb_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []

        lines = io_.from_file(file_name).split('\n')
        return [_check_shebang(file_name, lines)]


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
    action = _P1CheckShebang()
    lntr.run_action(action, args.files)


if __name__ == "__main__":
    _main(_parse())
