import logging
import os

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


class Test_list_1(hut.TestCase):
    def test_find_duplicates1(self) -> None:
        list_ = "a b c d".split()
        list_out = hlist.find_duplicates(list_)
        self.assertEqual(list_out, [])

    def test_find_duplicates2(self) -> None:
        list_ = "a b c a d e f f".split()
        list_out = hlist.find_duplicates(list_)
        self.assertEqual(set(list_out), set("a f".split()))

    def test_remove_duplicates1(self) -> None:
        list_ = "a b c d".split()
        list_out = hlist.remove_duplicates(list_)
        self.assertEqual(list_out, "a b c d".split())

    def test_remove_duplicates2(self) -> None:
        list_ = "a b c a d e f f".split()
        list_out = hlist.remove_duplicates(list_)
        self.assertEqual(list_out, "a b c d e f".split())

    def test_remove_duplicates3(self) -> None:
        list_ = "a b c a d e f f".split()
        list_ = list(reversed(list_))
        list_out = hlist.remove_duplicates(list_)
        self.assertEqual(list_out, "f e d a c b".split())
