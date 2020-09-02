import argparse
import logging
import re
from typing import List

import dev_scripts.linter2.base as lntr
import dev_scripts.linter2.utils as utils
import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr

_LOG = logging.getLogger(__name__)


def _format_separating_line(
    line: str, min_num_chars: int = 6, line_width: int = 78
) -> str:
    """Transform a line int oa separating line if more than 6 # are found.

    :param line: line to format
    :param min_num_chars: minimum number of # to match after '# ' to decide
    this is a seperator line
    :param line_width: desired width for the seperator line
    :return: modified line
    """
    regex = r"(\s*\#)\s*([\#\=\-\<\>]){%d,}\s*$" % min_num_chars

    m = re.match(regex, line)
    if m:
        char = m.group(2)
        line = m.group(1) + " " + char * (line_width - len(m.group(1)))
    return line


class _P1FormatSeparatingLine(lntr.Action):
    """Format separating lines."""

    def check_if_possible(self) -> bool:
        return True

    def _execute(self, file_name: str, pedantic: int) -> List[str]:
        _ = pedantic
        if not utils.is_py_file(file_name):
            _LOG.debug("Skipping file_name='%s'", file_name)
            return []

        lines = io_.from_file(file_name).split("\n")
        updated_lines = [_format_separating_line(line) for line in lines]

        utils.write_file_back(file_name, lines, updated_lines)
        return []


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
    action = _P1FormatSeparatingLine()
    lntr.run_action(action, args.files)


if __name__ == "__main__":
    _main(_parse())
