#!/usr/bin/env python
r"""

> p1_check_import.py sample-file1.py sample-file2.py
"""
import re
import argparse
import logging

from typing import List

import dev_scripts.linter2.utils as utils
import dev_scripts.linter2.base as lntr
import helpers.io_ as io_
import helpers.dbg as dbg
import helpers.parser as prsr


_LOG = logging.getLogger(__name__)


def _check_import(file_name: str, line_num: int, line: str) -> str:
    # The maximum length of an 'import as'.
    MAX_LEN_IMPORT = 8

    msg = ""

    if utils.is_init_py(file_name):
        # In **init**.py we can import in weird ways. (e.g., the evil
        # `from ... import *`).
        return msg

    m = re.match(r"\s*from\s+(\S+)\s+import\s+.*", line)
    if m:
        if m.group(1) != "typing":
            msg = "%s:%s: do not use '%s' use 'import foo.bar " "as fba'" % (
                file_name,
                line_num,
                line.rstrip().lstrip(),
            )
    else:
        m = re.match(r"\s*import\s+\S+\s+as\s+(\S+)", line)
        if m:
            shortcut = m.group(1)
            if len(shortcut) > MAX_LEN_IMPORT:
                msg = (
                        "%s:%s: the import shortcut '%s' in '%s' is longer than "
                        "%s characters"
                        % (
                            file_name,
                            line_num,
                            shortcut,
                            line.rstrip().lstrip(),
                            MAX_LEN_IMPORT,
                        )
                )
    return msg


class _P1CheckImport(lntr.Action):
    def check_if_possible(self) -> bool:
        return True

    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        _ = pedantic
        if not utils.is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []

        output = []

        lines = io_.from_file(file_name).split('\n')
        for i, line in enumerate(lines):
            msg = _check_import(file_name, i, line)
            if msg:
                output.append(msg)

        return output


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
    action = _P1CheckImport()
    lntr.run_action(action, args.files)


if __name__ == "__main__":
    _main(_parse())
