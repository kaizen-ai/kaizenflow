import logging

import numpy as np
import pandas as pd
import pytest

import core.explore as exp
import helpers.datetime_ as hdatetim
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_explore1(hut.TestCase):
    def test_ols_regress_series(self) -> None:
        x = 5 * np.random.randn(100)
        y = x + np.random.randn(*x.shape)
        df = pd.DataFrame()
        df["x"] = x
        df["y"] = y
        exp.ols_regress_series(
            df["x"], df["y"], intercept=True, print_model_stats=False
        )

    @pytest.mark.skip(reason="https://github.com/.../.../issues/3676")
    def test_rolling_pca_over_time1(self) -> None:
        np.random.seed(42)
        df = pd.DataFrame(np.random.randn(10, 5))
        df.index = pd.date_range("2017-01-01", periods=10)
        corr_df, eigval_df, eigvec_df = exp.rolling_pca_over_time(
            df, 0.5, "fill_with_zero"
        )
        txt = (
            "corr_df=\n%s\n" % corr_df.to_string()
            + "eigval_df=\n%s\n" % eigval_df.to_string()
            + "eigvec_df=\n%s\n" % eigvec_df.to_string()
        )
        self.check_string(txt)


class TestFilterByTime(hut.TestCase):
    @staticmethod
    def _get_test_data() -> pd.DataFrame:
        """
        Get data for testing.

        :return: data for testing
        """
        df = pd.DataFrame({
            "col1": [1, 2, 3, 4],
            "col2": [
                hdatetim.to_datetime("2018-04-05"),
                hdatetim.to_datetime("2018-04-06"),
                hdatetim.to_datetime("2018-04-07"),
                hdatetim.to_datetime("2018-04-08"),
            ]
        })
        df.index = pd.date_range("2017-01-01", periods=4)
        return df

    def test_filter_by_index1(self) -> None:
        """
        Verify that lower bound is included.
        """
        df = self._get_test_data()
        lower_bound = hdatetim.to_datetime("2017-01-02")
        upper_bound = hdatetim.to_datetime("2017-01-10")
        actual = exp.filter_by_time(
            df=df,
            ts_col_name="index",
            lower_close_interval=lower_bound,
            upper_close_interval=upper_bound,
        )
        expected = df[1::]
        self.assert_equal(actual.to_string(), expected.to_string())

    def test_filter_by_index2(self) -> None:
        """
        Verify that upper bound is not included.
        """
        df = self._get_test_data()
        lower_bound = hdatetim.to_datetime("2017-01-02")
        upper_bound = hdatetim.to_datetime("2017-01-04")
        actual = exp.filter_by_time(
            df=df,
            ts_col_name="index",
            lower_close_interval=lower_bound,
            upper_close_interval=upper_bound,
        )
        expected = df[1:3]
        self.assert_equal(actual.to_string(), expected.to_string())

    def test_filter_by_column1(self) -> None:
        """
        Verify that lower bound is included.
        """
        df = self._get_test_data()
        lower_bound = hdatetim.to_datetime("2018-04-06")
        upper_bound = hdatetim.to_datetime("2018-04-10")
        actual = exp.filter_by_time(
            df=df,
            ts_col_name="col2",
            lower_close_interval=lower_bound,
            upper_close_interval=upper_bound,
        )
        expected = df[1::]
        self.assert_equal(actual.to_string(), expected.to_string())

    def test_filter_by_column2(self) -> None:
        """
        Verify that upper bound is not included.
        """
        df = self._get_test_data()
        lower_bound = hdatetim.to_datetime("2018-04-06")
        upper_bound = hdatetim.to_datetime("2018-04-08")
        actual = exp.filter_by_time(
            df=df,
            ts_col_name="col2",
            lower_close_interval=lower_bound,
            upper_close_interval=upper_bound,
        )
        expected = df[1:3]
        self.assert_equal(actual.to_string(), expected.to_string())

    def test_no_intersection(self) -> None:
        """
        Verify that if time interval is not covered by data then empty DataFrame is returned.
        """
        df = self._get_test_data()
        lower_bound = hdatetim.to_datetime("2021-04-06")
        upper_bound = hdatetim.to_datetime("2021-04-08")
        actual = exp.filter_by_time(
            df=df,
            ts_col_name="index",
            lower_close_interval=lower_bound,
            upper_close_interval=upper_bound,
        )
        self.assertEqual(actual.shape[0], 0)

