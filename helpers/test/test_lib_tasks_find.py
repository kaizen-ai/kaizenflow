import logging
import os
import re
from typing import Dict, List

import invoke
import pytest

import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hprint as hprint
import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest
import helpers.lib_tasks as hlibtask

_LOG = logging.getLogger(__name__)


class Test_find_short_import1(hunitest.TestCase):
    def test1(self) -> None:
        iterator = [
            ("file1.py", 10, "import dataflow.core.dag_runner as dtfcodarun"),
            ("file1.py", 11, "import helpers.hpandas as hpandas"),
        ]
        results = hlibtask._find_short_import(iterator, "dtfcodarun")
        act = "\n".join(map(str, results))
        exp = r"""('file1.py', 10, 'import dataflow.core.dag_runner as dtfcodarun', 'dtfcodarun', 'import dataflow.core.dag_runner as dtfcodarun')"""
        self.assert_equal(act, exp, fuzzy_match=True)


class Test_find_func_class_uses1(hunitest.TestCase):
    def test1(self) -> None:
        iterator = [
            (
                "file1.py",
                10,
                "dag_runner = dtfsys.RealTimeDagRunner(**dag_runner_kwargs)",
            ),
            (
                "file1.py",
                11,
                "This test is similar to `TestRealTimeDagRunner1`. It uses:",
            ),
            ("file1.py", 12, "dag_builder: dtfcodabui.DagRunner,"),
            ("file1.py", 13, ":param dag_builder: `DagRunner` instance"),
        ]
        results = hlibtask._find_func_class_uses(iterator, "DagRunner")
        act = "\n".join(map(str, results))
        exp = r"""
        ('file1.py', 10, 'dag_runner = dtfsys.RealTimeDagRunner(**dag_runner_kwargs)', 'dtfsys', 'RealTimeDagRunner')
        ('file1.py', 12, 'dag_builder: dtfcodabui.DagRunner,', 'dtfcodabui', 'DagRunner')"""
        self.assert_equal(act, exp, fuzzy_match=True)
