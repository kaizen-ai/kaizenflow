import logging
from typing import Any, Dict, List, Match, Tuple

import helpers.dbg as dbg
import helpers.printing as hprint

_LOG = logging.getLogger(__name__)

# TODO(gp): Move to csv_helpers.py (but without introducing the dependencies from pandas).
TABLE = List[List[str]]

def check_table(table: TABLE, cols: List[str]) -> None:
    """
    Check whether the table is welformed (e.g., the list is rectangular).
    """
    for row in table:
        dbg.dassert_eq(len(row), len(cols),
                       "Invalid row='%s' for cols='%s'", row, cols)


def size(table: TABLE) -> Tuple[int, int]:
    """
    :return: number of columns x number of rows (same as numpy and pandas convention)
    """

    return len(table), len(table[0])


def table_to_string(table: TABLE) -> str:
    # Convert the cells to strings.
    table_as_str = [[str(cell) for cell in row] for row in table]
    # Find the length of each columns.
    lens = [max(map(len, col)) for col in zip(*table_as_str)]
    _LOG.debug(hprint.to_str("lens"))
    # Print format.
    fmt = '\t'.join('{{:{}}}'.format(x) for x in lens)
    table = [fmt.format(*row) for row in table_as_str]
    #
    res = '\n'.join(table)
    return res


def filter_table(table: TABLE, cols: List[str], field: str, value: str) -> TABLE:
    _LOG.debug(hprint.to_str("table"))
    table_to_string(table)
    # Sanity check.
    check_table(table, cols)
    #
    col_to_idx = {col: idx for idx, col in enumerate(cols)}
    #_LOG.debug("col_to_idx=%s", str(col_to_idx))
    table = [row for row in table if row [col_to_idx[field]] == value]
    _LOG.debug(hprint.to_str("table"))
    return table
