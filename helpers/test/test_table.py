import logging
from typing import Any, Dict, List, Tuple

import helpers.dbg as dbg
import helpers.unit_test as hut
import helpers.printing as hprint
import helpers.table as htable

_LOG = logging.getLogger(__name__)


class TestTable1(hut.TestCase):
    @staticmethod
    def _get_table() -> htable.Table:
        txt = """completed failure Lint Run_linter
completed success Lint Fast_tests
completed success Lint Slow_tests"""
        cols = ["status", "outcome", "descr", "workflow"]
        #table = [line for line in csv.reader(txt.split("\n"), delimiter=' ')]
        #_LOG.debug(hprint.to_str("table"))
        #_LOG.debug("size=%s", str(htable.size(table)))
        table = htable.Table.from_text(cols, txt, delimiter=" ")
        return table

    def test_from_text1(self) -> None:
        table = self._get_table()
        self.assertIsInstance(table, htable.Table)
        _LOG.debug(hprint.to_str("table"))


    def test_table_to_string1(self) -> None:
        table = self._get_table()
        act = str(table)
        exp = r"""
completed       failure Lint    Run_linter
completed       success Lint    Fast_tests
completed       success Lint    Slow_tests
"""
        exp = exp.rstrip().lstrip()
        self.assert_equal(act, exp, fuzzy_match=False)
