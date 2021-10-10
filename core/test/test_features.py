import io
import logging

import pandas as pd

import core.features as cfea
import helpers.unit_test as huntes

_LOG = logging.getLogger(__name__)


class Test_cross_feature_pairs(huntes.TestCase):
    def test1(self) -> None:
        df = self._get_df()
        actual = cfea.cross_feature_pairs(
            df,
            [
                ("x1", "x2", ["difference"], "x1x2"),
                ("x3", "x4", ["compressed_mean"], "x3x4"),
            ],
        )
        txt = """
datetime,x1x2.difference,x3x4.compressed_mean
2016-01-04 12:00:00,-1.414214,0.012021
2016-01-04 12:01:00,-0.707107,NaN
2016-01-04 12:02:00,1.414214,0.002546
2016-01-04 12:03:00,NaN,0.002616
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        self.assert_dfs_close(actual, expected, rtol=1e-6, atol=1e-6)

    @staticmethod
    def _get_df() -> pd.DataFrame:
        txt = """
datetime,x1,x2,x3,x4
2016-01-04 12:00:00,-1,1,0.002,0.015
2016-01-04 12:01:00,0,1,0.0015,NaN
2016-01-04 12:02:00,1,-1,0.0017,0.0019
2016-01-04 12:03:00,NaN,-1,0.0021,0.0016
"""
        df = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        return df


class Test_cross_feature_pair(huntes.TestCase):
    def test_difference1(self) -> None:
        df = self._get_df()
        actual = cfea.cross_feature_pair(df, "x1", "x2", ["difference"])
        txt = """
datetime,difference
2016-01-04 12:00:00,-1.414214
2016-01-04 12:01:00,-0.707107
2016-01-04 12:02:00,1.414214
2016-01-04 12:03:00,NaN
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        self.assert_dfs_close(actual, expected, rtol=1e-6, atol=1e-6)

    def test_difference2(self) -> None:
        df = self._get_df()
        actual = cfea.cross_feature_pair(df, "x3", "x4", ["difference"])
        txt = """
datetime,difference
2016-01-04 12:00:00,-0.009192
2016-01-04 12:01:00,NaN
2016-01-04 12:02:00,-0.000141
2016-01-04 12:03:00,0.000354
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        self.assert_dfs_close(actual, expected, rtol=1e-6, atol=1e-6)

    def test_compressed_difference1(self) -> None:
        df = self._get_df()
        actual = cfea.cross_feature_pair(
            df, "x1", "x2", ["compressed_difference"]
        )
        txt = """
datetime,compressed_difference
2016-01-04 12:00:00,-1.358092
2016-01-04 12:01:00,-0.699832
2016-01-04 12:02:00,1.358092
2016-01-04 12:03:00,NaN
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        self.assert_dfs_close(actual, expected, rtol=1e-6, atol=1e-6)

    def test_compressed_difference2(self) -> None:
        df = self._get_df()
        actual = cfea.cross_feature_pair(
            df, "x3", "x4", ["compressed_difference"]
        )
        txt = """
datetime,compressed_difference
2016-01-04 12:00:00,-0.009192
2016-01-04 12:01:00,NaN
2016-01-04 12:02:00,-.000141
2016-01-04 12:03:00,0.000354
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        self.assert_dfs_close(actual, expected, rtol=1e-6, atol=1e-6)

    def test_normalized_difference1(self) -> None:
        df = self._get_df()
        actual = cfea.cross_feature_pair(
            df, "x1", "x2", ["normalized_difference"]
        )
        txt = """
datetime,normalized_difference
2016-01-04 12:00:00,-1.0
2016-01-04 12:01:00,-1.0
2016-01-04 12:02:00,1.0
2016-01-04 12:03:00,NaN
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        self.assert_dfs_close(actual, expected, atol=1e-7)

    def test_normalized_difference2(self) -> None:
        df = self._get_df()
        actual = cfea.cross_feature_pair(
            df, "x3", "x4", ["normalized_difference"]
        )
        txt = """
datetime,normalized_difference
2016-01-04 12:00:00,-0.764706
2016-01-04 12:01:00,NaN
2016-01-04 12:02:00,-0.055556
2016-01-04 12:03:00,0.135135
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        self.assert_dfs_close(actual, expected, rtol=1e-6, atol=1e-6)

    def test_difference_of_logs(self) -> None:
        df = self._get_df()
        actual = cfea.cross_feature_pair(df, "x3", "x4", ["difference_of_logs"])
        txt = """
datetime,difference_of_logs
2016-01-04 12:00:00,-2.014903
2016-01-04 12:01:00,NaN
2016-01-04 12:02:00,-0.111226
2016-01-04 12:03:00,0.271934
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        self.assert_dfs_close(actual, expected, rtol=1e-6, atol=1e-6)

    def test_mean(self) -> None:
        df = self._get_df()
        actual = cfea.cross_feature_pair(df, "x3", "x4", ["mean"])
        txt = """
datetime,mean
2016-01-04 12:00:00,0.012021
2016-01-04 12:01:00,NaN
2016-01-04 12:02:00,0.002546
2016-01-04 12:03:00,0.002616
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        self.assert_dfs_close(actual, expected, rtol=1e-6, atol=1e-6)

    def test_compressed_mean(self) -> None:
        df = self._get_df()
        actual = cfea.cross_feature_pair(df, "x3", "x4", ["compressed_mean"])
        txt = """
datetime,compressed_mean
2016-01-04 12:00:00,0.012021
2016-01-04 12:01:00,NaN
2016-01-04 12:02:00,0.002546
2016-01-04 12:03:00,0.002616
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        self.assert_dfs_close(actual, expected, rtol=1e-6, atol=1e-6)

    def test_mean_of_logs(self) -> None:
        df = self._get_df()
        actual = cfea.cross_feature_pair(df, "x3", "x4", ["mean_of_logs"])
        txt = """
datetime,mean_of_logs
2016-01-04 12:00:00,-5.207157
2016-01-04 12:01:00,NaN
2016-01-04 12:02:00,-6.321514
2016-01-04 12:03:00,-6.301785
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        self.assert_dfs_close(actual, expected, rtol=1e-6, atol=1e-6)

    @staticmethod
    def _get_df() -> pd.DataFrame:
        txt = """
datetime,x1,x2,x3,x4
2016-01-04 12:00:00,-1,1,0.002,0.015
2016-01-04 12:01:00,0,1,0.0015,NaN
2016-01-04 12:02:00,1,-1,0.0017,0.0019
2016-01-04 12:03:00,NaN,-1,0.0021,0.0016
"""
        df = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        return df
