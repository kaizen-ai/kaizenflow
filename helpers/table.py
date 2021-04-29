import copy
import csv
import logging
from typing import List, Tuple

import helpers.dbg as dbg
import helpers.printing as hprint

_LOG = logging.getLogger(__name__)

# TODO(gp): Move to csv_helpers.py (but without introducing the dependencies from
#  pandas).
TABLE = List[List[str]]


class Table:
    """
    A simple (rectangular) table without introducing a dependency from pandas.
    """

    def __init__(self, table: TABLE, cols: List[str]) -> None:
        self._check_table(table, cols)
        self._table = table
        self._cols = cols
        _LOG.debug("%s", self.__repr__())
        self._col_to_idx = {col: idx for idx, col in enumerate(self._cols)}
        _LOG.debug("col_to_idx=%s", str(self._col_to_idx))

    @classmethod
    def from_text(cls, cols: List[str], txt: str, delimiter: str) -> "Table":
        dbg.dassert_isinstance(txt, str)
        table = [line for line in csv.reader(txt.split("\n"), delimiter=delimiter)]
        return cls(table, cols)

    def size(self) -> Tuple[int, int]:
        """
        Return the size of the table.

        :return: number of columns x number of rows (same as numpy and pandas convention)
        """
        dbg.dassert_lte(1, len(self._table))
        return len(self._table), len(self._table[0])

    def __str__(self) -> str:
        """
        Return a string representing the table with columns aligned.
        """
        table = copy.deepcopy(self._table)
        table.insert(0, self._cols)
        # Convert the cells to strings.
        table_as_str = [[str(cell) for cell in row] for row in table]
        # Find the length of each columns.
        lens = [max(map(len, col)) for col in zip(*table_as_str)]
        _LOG.debug(hprint.to_str("lens"))
        # Compute format for the columns.
        fmt = ' '.join('{{:{}}}'.format(x) for x in lens)
        _LOG.debug(hprint.to_str("fmt"))
        # Format rows.
        table = [fmt.format(*row) for row in table_as_str]
        # Create string.
        res = '\n'.join(table)
        res += "size=%s", str(self.size())
        return res

    def __repr__(self) -> str:
        res = ""
        res += "table=\n%s", "\n".join(map(str, self._table)))
        res += "cols=%s", ", ".join(self._cols)
        return res

    def filter_table(self, field: str, value: str) -> "Table":
        """
        Return a Table filtered with the criteria "field == value".
        """
        _LOG.debug("self=%s", self.str())
        # Filter the rows.
        dbg.dassert_in(field, self._col_to_idx.keys())
        filtered_table = [row for row in self._table if
                          row[self._col_to_idx[field]] == value]
        # Build the resulting table.
        table = Table(filtered_table, self._cols)
        _LOG.debug("table=%s", str(table))
        _LOG.debug("table.size()=%s", str(table.size()))
        return table

    @staticmethod
    def _check_table(table: TABLE, cols: List[str]) -> None:
        """
        Check that the table is wellformed (e.g., the list of lists is rectangular).
        """
        dbg.dassert_isinstance(table, list)
        dbg.dassert_isinstance(cols, list)
        dbg.dassert_no_duplicates(cols)
        # Check that the list of lists is rectangular.
        for row in table:
            dbg.dassert_isinstance(table, list)
            dbg.dassert_eq(len(row), len(cols),
                           "Invalid row='%s' for cols='%s'", row, cols)
