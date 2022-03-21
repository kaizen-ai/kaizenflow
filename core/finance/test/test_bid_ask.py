import io
import logging

import numpy as np
import pandas as pd

import core.finance.bid_ask as cfibiask
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class Test_process_bid_ask(hunitest.TestCase):
    def test_mid(self) -> None:
        df = self._get_df()
        actual = cfibiask.process_bid_ask(
            df, "bid", "ask", "bid_volume", "ask_volume", ["mid"]
        )
        txt = """
datetime,mid
2016-01-04 12:00:00,100.015
2016-01-04 12:01:00,100.015
2016-01-04 12:02:00,100.000
2016-01-04 12:03:00,100.000
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        np.testing.assert_allclose(actual, expected)

    def test_geometric_mid(self) -> None:
        df = self._get_df()
        actual = cfibiask.process_bid_ask(
            df, "bid", "ask", "bid_volume", "ask_volume", ["geometric_mid"]
        )
        txt = """
datetime,geometric_mid
2016-01-04 12:00:00,100.01499987501875
2016-01-04 12:01:00,100.01499987501875
2016-01-04 12:02:00,99.9999995
2016-01-04 12:03:00,99.99999799999998
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        np.testing.assert_allclose(actual, expected)

    def test_quoted_spread(self) -> None:
        df = self._get_df()
        actual = cfibiask.process_bid_ask(
            df, "bid", "ask", "bid_volume", "ask_volume", ["quoted_spread"]
        )
        txt = """
datetime,quoted_spread
2016-01-04 12:00:00,0.01
2016-01-04 12:01:00,0.01
2016-01-04 12:02:00,0.02
2016-01-05 12:02:00,0.04
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        np.testing.assert_allclose(actual, expected)

    def test_relative_spread(self) -> None:
        df = self._get_df()
        actual = cfibiask.process_bid_ask(
            df, "bid", "ask", "bid_volume", "ask_volume", ["relative_spread"]
        )
        txt = """
datetime,relative_spread
2016-01-04 12:00:00,9.998500224957161e-05
2016-01-04 12:01:00,9.998500224957161e-05
2016-01-04 12:02:00,0.00020000000000010233
2016-01-04 12:03:00,0.00039999999999992044
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        np.testing.assert_allclose(actual, expected)

    def test_log_relative_spread(self) -> None:
        df = self._get_df()
        actual = cfibiask.process_bid_ask(
            df, "bid", "ask", "bid_volume", "ask_volume", ["log_relative_spread"]
        )
        txt = """
datetime,log_relative_spread
2016-01-04 12:00:00,9.998500233265872e-05
2016-01-04 12:01:00,9.998500233265872e-05
2016-01-04 12:02:00,0.00020000000066744406
2016-01-04 12:03:00,0.00040000000533346736
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        np.testing.assert_allclose(actual, expected)

    def test_weighted_mid(self) -> None:
        df = self._get_df()
        actual = cfibiask.process_bid_ask(
            df, "bid", "ask", "bid_volume", "ask_volume", ["weighted_mid"]
        )
        txt = """
datetime,weighted_mid
2016-01-04 12:00:00,100.015
2016-01-04 12:01:00,100.014
2016-01-04 12:02:00,100.000
2016-01-04 12:03:00,99.993333
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        np.testing.assert_allclose(actual, expected)

    def test_order_book_imbalance(self) -> None:
        df = self._get_df()
        actual = cfibiask.process_bid_ask(
            df, "bid", "ask", "bid_volume", "ask_volume", ["order_book_imbalance"]
        )
        txt = """
datetime,order_book_imbalance
2016-01-04 12:00:00,0.5
2016-01-04 12:01:00,0.4
2016-01-04 12:02:00,0.5
2016-01-04 12:03:00,0.3333333333
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        np.testing.assert_allclose(actual, expected)

    def test_centered_order_book_imbalance(self) -> None:
        df = self._get_df()
        actual = cfibiask.process_bid_ask(
            df,
            "bid",
            "ask",
            "bid_volume",
            "ask_volume",
            ["centered_order_book_imbalance"],
        )
        txt = """
datetime,centered_order_book_imbalance
2016-01-04 12:00:00,0.0
2016-01-04 12:01:00,-0.1999999999
2016-01-04 12:02:00,0.0
2016-01-04 12:03:00,-0.3333333333
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        np.testing.assert_allclose(actual, expected)

    def test_centered_order_book_imbalance(self) -> None:
        df = self._get_df()
        actual = cfibiask.process_bid_ask(
            df,
            "bid",
            "ask",
            "bid_volume",
            "ask_volume",
            ["log_order_book_imbalance"],
        )
        txt = """
datetime,centered_order_book_imbalance
2016-01-04 12:00:00,0.0
2016-01-04 12:01:00,-0.405465108
2016-01-04 12:02:00,0.0
2016-01-04 12:03:00,-0.693147181
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        np.testing.assert_allclose(actual, expected)

    def test_bid_value(self) -> None:
        df = self._get_df()
        actual = cfibiask.process_bid_ask(
            df, "bid", "ask", "bid_volume", "ask_volume", ["bid_value"]
        )
        txt = """
datetime,bid_value
2016-01-04 12:00:00,20002.0
2016-01-04 12:01:00,20002.0
2016-01-04 12:02:00,29997.0
2016-01-04 12:03:00,19996.0
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        np.testing.assert_allclose(actual, expected)

    def test_ask_value(self) -> None:
        df = self._get_df()
        actual = cfibiask.process_bid_ask(
            df, "bid", "ask", "bid_volume", "ask_volume", ["ask_value"]
        )
        txt = """
datetime,ask_value
2016-01-04 12:00:00,20004.0
2016-01-04 12:01:00,30006.0
2016-01-04 12:02:00,30003.0
2016-01-04 12:03:00,40008.0
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        np.testing.assert_allclose(actual, expected)

    def test_mid_value(self) -> None:
        df = self._get_df()
        actual = cfibiask.process_bid_ask(
            df, "bid", "ask", "bid_volume", "ask_volume", ["mid_value"]
        )
        txt = """
datetime,mid_value
2016-01-04 12:00:00,20003.0
2016-01-04 12:01:00,25004.0
2016-01-04 12:02:00,30000.0
2016-01-04 12:03:00,30002.0
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        np.testing.assert_allclose(actual, expected)

    @staticmethod
    def _get_df() -> pd.DataFrame:
        txt = """
datetime,bid,ask,bid_volume,ask_volume
2016-01-04 12:00:00,100.01,100.02,200,200
2016-01-04 12:01:00,100.01,100.02,200,300
2016-01-04 12:02:00,99.99,100.01,300,300
2016-01-04 12:03:00,99.98,100.02,200,400
"""
        df = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        return df
