import csv
import logging
from typing import Any, Dict, List

import helpers.dbg as dbg
import helpers.unit_test as hut
import helpers.printing as hprint
import helpers.table as htable

_LOG = logging.getLogger(__name__)


class TestTable1(hut.TestCase):
    def _get_table(self) -> Tuple[htable.TABLE, List[str]]:
        txt = """completed failure Lint Run_linter
completed success Lint Fast_tests
completed success Lint Slow_tests"""
        cols = ["status", "outcome", "descr", "workflow"]
        table = [line for line in csv.reader(txt.split("\n"), delimiter=' ')]
        return table, cols

    def test_example1(self):
