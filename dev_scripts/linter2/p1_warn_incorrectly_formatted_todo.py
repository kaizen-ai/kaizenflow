import re
import argparse
import logging

import dev_scripts.linter2.utils as utils
import dev_scripts.linter2.base as lntr
import helpers.parser as prsr
import helpers.dbg as dbg
import helpers.io_ as io_

from typing import List


_LOG = logging.getLogger(__name__)


def _warn_incorrectly_formatted_todo(
        file_name: str, line_num: int, line: str
) -> str:
    """Issues a warning for incorrectly formatted todo comments that don't
    match the format: (# TODO(assignee): (task).)"""
    msg = ""

    match = utils.parse_comment(line=line)
    if match is None:
        return msg

    comment = match.group(2)
    if not comment.lower().strip().startswith("todo"):
        return msg

    todo_regex = r"TODO\(\S+\): (.*)"

    match = re.search(todo_regex, comment)
    if match is None:
        msg = f"{file_name}:{line_num}: found incorrectly formatted TODO comment: '{comment}'"
    return msg


class _P1WarnIncorrectlyFormattedTodo(lntr.Action):
    """Apply p1 specific lints."""

    def check_if_possible(self) -> bool:
        return True

    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        _ = pedantic
        if not utils.is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []

        lines = io_.from_file(file_name).split('\n')
        output = []

        for i, line in enumerate(lines):
            msg = _warn_incorrectly_formatted_todo(file_name, i, line)
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
    action = _P1WarnIncorrectlyFormattedTodo()
    lntr.run_action(action, args.files)


if __name__ == "__main__":
    _main(_parse())
