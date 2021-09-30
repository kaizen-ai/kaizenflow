import logging
import os
from typing import List, Optional

import numpy as np
import pandas as pd

import helpers.csv_helpers as hchelp
import helpers.env as henv
import helpers.io_ as hio
import helpers.list as hlist
import helpers.printing as hprint
import helpers.s3 as hs3
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_list_find_duplicates1(hut.TestCase):
    def test1(self) -> None:
        list_ = "a b c d".split()
        list_out = hlist.find_duplicates(list_)
        self.assertEqual(list_out, [])

    def test2(self) -> None:
        list_ = "a b c a d e f f".split()
        list_out = hlist.find_duplicates(list_)
        self.assertEqual(set(list_out), set("a f".split()))


class Test_list_remove_duplicates1(hut.TestCase):

    def test1(self) -> None:
        list_ = "a b c d".split()
        list_out = hlist.remove_duplicates(list_)
        self.assertEqual(list_out, "a b c d".split())

    def test2(self) -> None:
        list_ = "a b c a d e f f".split()
        list_out = hlist.remove_duplicates(list_)
        self.assertEqual(list_out, "a b c d e f".split())

    def test3(self) -> None:
        list_ = "a b c a d e f f".split()
        list_ = list(reversed(list_))
        list_out = hlist.remove_duplicates(list_)
        self.assertEqual(list_out, "f e d a c b".split())


class Test_list_extract1(hut.TestCase):
    def _helper(self, start_idx: Optional[int], end_idx: Optional[int], expected_list: List[str]) -> None:
        list_ = "a b c d".split()
        actual_list = hlist.extract(list_, start_idx, end_idx)
        self.assertEqual(actual_list, expected_list)

    def test1(self) -> None:
        start_idx = 0
        end_idx = 1
        expected_list = "a".split()
        self._helper(start_idx, end_idx, expected_list)

    def test2(self) -> None:
        start_idx = 1
        end_idx = None
        expected_list = "b c d".split()
        self._helper(start_idx, end_idx, expected_list)

    def test3(self) -> None:
        start_idx = None
        end_idx = None
        expected_list = "a b c d".split()
        self._helper(start_idx, end_idx, expected_list)

    def test4(self) -> None:
        start_idx = None
        end_idx = 2
        expected_list = "a b".split()
        self._helper(start_idx, end_idx, expected_list)

    def test5(self) -> None:
        start_idx = None
        end_idx = 2
        expected_list = "a b".split()
        self._helper(start_idx, end_idx, expected_list)

    def test6(self) -> None:
        start_idx = 0
        end_idx = 4
        expected_list = "a b c d".split()
        self._helper(start_idx, end_idx, expected_list)

    def test7(self) -> None:
        start_idx = 0
        end_idx = 3
        expected_list = "a b c".split()
        self._helper(start_idx, end_idx, expected_list)


class Test_list_chunk1(hut.TestCase):
    def _helper(self, n: int, expected_list: List[str]) -> None:
        list_ = "a b c d e f".split()
        actual_list = hlist.chunk(list_, n)
        self.assertEqual(actual_list, expected_list)

    def test1(self) -> None:
        n = 1
        expected_list = ["a b c d e f".split()]
        self._helper(n, expected_list)

    def test2(self) -> None:
        n = 2
        expected_list = [["a", "b", "c"], ["d", "e", "f"]]
        self._helper(n, expected_list)

    def test3(self) -> None:
        n = 3
        expected_list = [["a", "b"], ["c", "d"], ["e", "f"]]
        self._helper(n, expected_list)

    def test4(self) -> None:
        n = 4
        expected_list = [["a", "b"], ["c", "d"], ["e"], ["f"]]
        self._helper(n, expected_list)

    def test5(self) -> None:
        n = 6
        expected_list = [["a"], ["b"], ["c"], ["d"], ["e"], ["f"]]
        self._helper(n, expected_list)
