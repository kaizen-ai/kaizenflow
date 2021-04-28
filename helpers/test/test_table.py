import csv
import logging
from typing import Any, Dict, List, Tuple

import helpers.dbg as dbg
import helpers.unit_test as hut
import helpers.printing as hprint
import helpers.table as htable

_LOG = logging.getLogger(__name__)


class TestTable1(hut.TestCase):
    @staticmethod
    def _get_table() -> Tuple[htable.TABLE, List[str]]:
        txt = """completed failure Lint Run_linter
completed success Lint Fast_tests
completed success Lint Slow_tests"""
        cols = ["status", "outcome", "descr", "workflow"]
        table = [line for line in csv.reader(txt.split("\n"), delimiter=' ')]
        _LOG.debug(hprint.to_str("table"))
        _LOG.debug("size=%s", str(htable.size(table)))
        return table, cols

    def test_table_to_string1(self) -> None:
        table, cols = self._get_table()
        act = htable.table_to_string(table)
        exp = r"""
completed       failure Lint    Run_linter
completed       success Lint    Fast_tests
completed       success Lint    Slow_tests
"""
        exp = exp.rstrip().lstrip()
        self.assert_equal(act, exp, fuzzy_match=False)
