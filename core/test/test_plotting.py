import logging
import unittest

import core.plotting as plot
import pandas as pd

_LOG = logging.getLogger(__name__)


class Test_select_series_to_remove(unittest.TestCase):
    def test_select_series_to_remove1(self) -> None:
        """
        Test correct output of df correlation with threashold = 0.7
        """
        data = {
            "A": [45, 37, 42, 35, 39],
            "B": [38, 31, 26, 28, 33],
            "C": [10, 15, 17, 21, 12],
            "D": [9, 13, 16, 15, 12],
            "E": [9, 9, 7, 5, 10],
        }

        df = pd.DataFrame(data, columns=["A", "B", "C", "D", "E"])
        expected = ["C", "B"]
        actual = plot.select_series_to_remove(df.corr(), 0.7)
        self.assertListEqual(actual, expected)

    def test_select_series_to_remove2(self) -> None:
        """
        Test correct output of df correlation with threashold = 0.5
        """
        data = {
            "A": [45, 37, 42, 35, 39],
            "B": [38, 31, 26, 28, 33],
            "C": [10, 15, 17, 21, 12],
            "D": [9, 13, 16, 15, 12],
            "E": [9, 9, 7, 5, 10],
        }

        df = pd.DataFrame(data, columns=["A", "B", "C", "D", "E"])
        expected = ["B", "C", "D"]
        actual = plot.select_series_to_remove(df.corr(), 0.5)
        self.assertListEqual(actual, expected)

    def test_select_series_to_remove3(self) -> None:
        """
        Test correct output of df correlation with threashold = 0.3
        """
        data = {
            "A": [45, 37, 42, 35, 39],
            "B": [38, 31, 26, 28, 33],
            "C": [10, 15, 17, 21, 12],
            "D": [9, 13, 16, 15, 12],
            "E": [9, 9, 7, 5, 10],
        }

        df = pd.DataFrame(data, columns=["A", "B", "C", "D", "E"])
        expected = ["A", "B", "C", "D"]
        actual = plot.select_series_to_remove(df.corr(), 0.3)
        self.assertListEqual(actual, expected)
