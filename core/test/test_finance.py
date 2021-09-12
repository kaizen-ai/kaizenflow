import datetime
import io
import logging
from typing import Optional

import numpy as np
import pandas as pd

import core.artificial_signal_generators as sig_gen
import core.finance as fin
import core.signal_processing as sigp
import helpers.printing as prnt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_set_non_ath_to_nan1(hut.TestCase):
    def test1(self) -> None:
        """
        Test for active trading hours in [9:30, 16:00].
        """
        df = self._get_df()
        start_time = datetime.time(9, 30)
        end_time = datetime.time(16, 0)
        act = fin.set_non_ath_to_nan(df, start_time, end_time)
        exp = """
                              open   high    low  close        vol
        datetime
        2016-01-05 09:28:00    NaN    NaN    NaN    NaN        NaN
        2016-01-05 09:29:00    NaN    NaN    NaN    NaN        NaN
        2016-01-05 09:30:00    NaN    NaN    NaN    NaN        NaN
        2016-01-05 09:31:00  98.01  98.19  98.00  98.00   172584.0
        2016-01-05 09:32:00  97.99  98.04  97.77  97.77   189058.0
        2016-01-05 15:58:00  95.31  95.32  95.22  95.27   456235.0
        2016-01-05 15:59:00  95.28  95.36  95.22  95.32   729315.0
        2016-01-05 16:00:00  95.32  95.40  95.32  95.40  3255752.0
        2016-01-05 16:01:00    NaN    NaN    NaN    NaN        NaN
        2016-01-05 16:02:00    NaN    NaN    NaN    NaN        NaN
        """
        self.assert_equal(str(act), exp, fuzzy_match=True)

    @staticmethod
    def _get_df() -> pd.DataFrame:
        # From `s3://*****-data/data/kibot/all_stocks_1min/AAPL.csv.gz`.
        txt = """
datetime,open,high,low,close,vol
2016-01-05 09:28:00,98.0,98.05,97.99,98.05,2241
2016-01-05 09:29:00,98.04,98.13,97.97,98.13,14174
2016-01-05 09:30:00,98.14,98.24,97.79,98.01,751857
2016-01-05 09:31:00,98.01,98.19,98.0,98.0,172584
2016-01-05 09:32:00,97.99,98.04,97.77,97.77,189058
2016-01-05 15:58:00,95.31,95.32,95.22,95.27,456235
2016-01-05 15:59:00,95.28,95.36,95.22,95.32,729315
2016-01-05 16:00:00,95.32,95.4,95.32,95.4,3255752
2016-01-05 16:01:00,95.4,95.42,95.4,95.42,4635
2016-01-05 16:02:00,95.41,95.41,95.37,95.4,3926
"""
        df = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        return df


class Test_remove_times_outside_window(hut.TestCase):
    def test_remove(self) -> None:
        df = self._get_df()
        start_time = datetime.time(9, 29)
        end_time = datetime.time(16, 0)
        actual = fin.remove_times_outside_window(df, start_time, end_time)
        expected_txt = """
datetime,open,high,low,close,vol
2016-01-05 09:30:00,98.14,98.24,97.79,98.01,751857
2016-01-05 09:31:00,98.01,98.19,98.0,98.0,172584
2016-01-05 09:32:00,97.99,98.04,97.77,97.77,189058
2016-01-05 15:58:00,95.31,95.32,95.22,95.27,456235
2016-01-05 15:59:00,95.28,95.36,95.22,95.32,729315
2016-01-05 16:00:00,95.32,95.4,95.32,95.4,3255752
        """
        expected = pd.read_csv(
            io.StringIO(expected_txt), index_col=0, parse_dates=True
        )
        self.assert_dfs_close(actual, expected)

    def test_bypass(self) -> None:
        df = self._get_df()
        start_time = datetime.time(9, 29)
        end_time = datetime.time(16, 0)
        actual = fin.remove_times_outside_window(
            df, start_time, end_time, bypass=True
        )
        self.assert_dfs_close(actual, df)

    @staticmethod
    def _get_df() -> pd.DataFrame:
        # From `s3://*****-data/data/kibot/all_stocks_1min/AAPL.csv.gz`.
        txt = """
datetime,open,high,low,close,vol
2016-01-05 09:28:00,98.0,98.05,97.99,98.05,2241
2016-01-05 09:29:00,98.04,98.13,97.97,98.13,14174
2016-01-05 09:30:00,98.14,98.24,97.79,98.01,751857
2016-01-05 09:31:00,98.01,98.19,98.0,98.0,172584
2016-01-05 09:32:00,97.99,98.04,97.77,97.77,189058
2016-01-05 15:58:00,95.31,95.32,95.22,95.27,456235
2016-01-05 15:59:00,95.28,95.36,95.22,95.32,729315
2016-01-05 16:00:00,95.32,95.4,95.32,95.4,3255752
2016-01-05 16:01:00,95.4,95.42,95.4,95.42,4635
2016-01-05 16:02:00,95.41,95.41,95.37,95.4,3926
"""
        df = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        return df


class Test_set_weekends_to_nan(hut.TestCase):
    def test1(self) -> None:
        """
        Test for a daily frequency input.
        """
        mn_process = sig_gen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=2, seed=1)
        df = mn_process.generate_sample(
            {"start": "2020-01-01", "periods": 40, "freq": "D"}, seed=1
        )
        actual = fin.set_weekends_to_nan(df)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        """
        Test for a minutely frequency input.
        """
        mn_process = sig_gen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=2, seed=1)
        df = mn_process.generate_sample(
            {"start": "2020-01-05 23:00:00", "periods": 100, "freq": "T"}, seed=1
        )
        actual = fin.set_weekends_to_nan(df)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)


class Test_remove_weekends(hut.TestCase):
    def test_remove(self) -> None:
        df = self._get_df()
        actual = fin.remove_weekends(df)
        expected_txt = """
datetime,close,volume
2016-01-01,NaN,NaN
2016-01-04,100.00,100000
2016-01-05,100.00,100000
2016-01-06,100.00,100000
2016-01-07,100.00,100000
2016-01-08,100.00,100000
2016-01-11,100.00,100000
2016-01-12,100.00,100000
"""
        expected = pd.read_csv(
            io.StringIO(expected_txt), index_col=0, parse_dates=True
        )
        self.assert_dfs_close(actual, expected)

    def test_bypass(self) -> None:
        df = self._get_df()
        actual = fin.remove_weekends(df, bypass=True)
        self.assert_dfs_close(actual, df)

    @staticmethod
    def _get_df() -> pd.DataFrame:
        txt = """
datetime,close,volume
2016-01-01,NaN,NaN
2016-01-02,NaN,NaN
2016-01-03,NaN,NaN
2016-01-04,100.00,100000
2016-01-05,100.00,100000
2016-01-06,100.00,100000
2016-01-07,100.00,100000
2016-01-08,100.00,100000
2016-01-09,100.00,100000
2016-01-10,100.00,100000
2016-01-11,100.00,100000
2016-01-12,100.00,100000
"""
        df = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        return df


class Test_resample_time_bars1(hut.TestCase):
    def test1(self) -> None:
        """
        Resampling with the same frequency of the data should not change
        anything.
        """
        df = self._get_df()
        #
        rule = "1T"
        df_out = self._resample_helper(df, rule)
        # Check output.
        act = self._compute_actual_output(df, df_out)
        exp = """
        df
                             close      vol     ret_0
        datetime
        2016-01-04 09:30:00  94.70  1867590       NaN
        2016-01-04 09:31:00  94.98   349119  0.002957
        2016-01-04 09:32:00  95.33   419479  0.003685
        2016-01-04 09:33:00  95.03   307383 -0.003147
        2016-01-04 09:34:00  94.89   342218 -0.001473
        2016-01-04 09:35:00  94.97   358280  0.000843
        2016-01-04 09:36:00  95.21   266199  0.002527
        2016-01-04 09:37:00  95.48   293074  0.002836
        2016-01-04 09:38:00  95.95   581584  0.004922
        2016-01-04 09:39:00  96.07   554872  0.001251
        df_out
                                ret_0  close      vol
        datetime
        2016-01-04 09:30:00       NaN  94.70  1867590
        2016-01-04 09:31:00  0.002957  94.98   349119
        2016-01-04 09:32:00  0.003685  95.33   419479
        2016-01-04 09:33:00 -0.003147  95.03   307383
        2016-01-04 09:34:00 -0.001473  94.89   342218
        2016-01-04 09:35:00  0.000843  94.97   358280
        2016-01-04 09:36:00  0.002527  95.21   266199
        2016-01-04 09:37:00  0.002836  95.48   293074
        2016-01-04 09:38:00  0.004922  95.95   581584
        2016-01-04 09:39:00  0.001251  96.07   554872
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    def test2(self) -> None:
        """
        Resample data with 1 min resolution with 5 mins intervals.
        """
        df = self._get_df()
        #
        rule = "5T"
        df_out = self._resample_helper(df, rule)
        # Check output.
        act = self._compute_actual_output(df, df_out)
        exp = """
        df
                             close      vol     ret_0
        datetime
        2016-01-04 09:30:00  94.70  1867590       NaN
        2016-01-04 09:31:00  94.98   349119  0.002957
        2016-01-04 09:32:00  95.33   419479  0.003685
        2016-01-04 09:33:00  95.03   307383 -0.003147
        2016-01-04 09:34:00  94.89   342218 -0.001473
        2016-01-04 09:35:00  94.97   358280  0.000843
        2016-01-04 09:36:00  95.21   266199  0.002527
        2016-01-04 09:37:00  95.48   293074  0.002836
        2016-01-04 09:38:00  95.95   581584  0.004922
        2016-01-04 09:39:00  96.07   554872  0.001251
        df_out
                                ret_0    close      vol
        datetime
        2016-01-04 09:30:00       NaN  94.7000  1867590
        2016-01-04 09:35:00  0.002865  95.0400  1776479
        2016-01-04 09:40:00  0.011536  95.6775  1695729
        """
        self.assert_equal(act, exp, fuzzy_match=True)
        # Check manually certain values for the returns.
        self.assertTrue(np.isnan(df.loc["2016-01-04 09:30:00", "ret_0"]))
        np.testing.assert_almost_equal(
            df.loc["2016-01-04 09:31:00", "ret_0"],
            (94.98 - 94.70) / 94.70,
            decimal=6,
        )
        np.testing.assert_almost_equal(
            df.loc["2016-01-04 09:39:00", "ret_0"],
            (96.07 - 95.95) / 95.95,
            decimal=6,
        )
        # The resampling is (a, b] with the label on b.
        # The first interval corresponds to (9:25, 9:30] and is timestamped with
        # 9:30am. The values are the same as the first row of the input.
        timestamp = "2016-01-04 09:30:00"
        for col in df.columns:
            np.testing.assert_almost_equal(
                df_out.loc[timestamp, col], df.loc[timestamp, col]
            )
        # The second interval corresponds to (9:30, 9:35] and is timestamped with
        # 9:35am.
        timestamp = "2016-01-04 09:35:00"
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "close"],
            np.mean([94.98, 95.33, 95.03, 94.89, 94.97]),
        )
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "vol"],
            np.sum([349119, 419479, 307383, 342218, 358280]),
        )
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "ret_0"],
            np.sum([0.002957, 0.003685, -0.003147, -0.001473, 0.000843]),
        )
        # The last interval corresponds to (9:35, 9:40] and is timestamped with 9:40am.
        # Note that the last timestamp (i.e., 9:40am) is missing so the average is
        # on 4 values and not 1.
        timestamp = "2016-01-04 09:40:00"
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "close"], np.mean([95.21, 95.48, 95.95, 96.07])
        )
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "vol"], np.sum([266199, 293074, 581584, 554872])
        )
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "ret_0"],
            np.sum([0.002527, 0.002836, 0.004922, 0.001251]),
        )

    @staticmethod
    def _get_df() -> pd.DataFrame:
        # From `s3://*****-data/data/kibot/all_stocks_1min/AAPL.csv.gz`.
        txt = """
datetime,close,vol,ret_0
2016-01-04 09:30:00,94.7,1867590,NaN
2016-01-04 09:31:00,94.98,349119,0.002957
2016-01-04 09:32:00,95.33,419479,0.003685
2016-01-04 09:33:00,95.03,307383,-0.003147
2016-01-04 09:34:00,94.89,342218,-0.001473
2016-01-04 09:35:00,94.97,358280,0.000843
2016-01-04 09:36:00,95.21,266199,0.002527
2016-01-04 09:37:00,95.48,293074,0.002836
2016-01-04 09:38:00,95.95,581584,0.004922
2016-01-04 09:39:00,96.07,554872,0.001251
"""
        df = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        return df

    @staticmethod
    def _resample_helper(df: pd.DataFrame, rule: str) -> pd.DataFrame:
        return_cols = ["ret_0"]
        return_agg_func = None
        return_agg_func_kwargs = None
        #
        price_cols = ["close"]
        price_agg_func = None
        price_agg_func_kwargs = None
        #
        volume_cols = ["vol"]
        volume_agg_func = None
        volume_agg_func_kwargs = None
        df_out = fin.resample_time_bars(
            df,
            rule,
            return_cols=return_cols,
            return_agg_func=return_agg_func,
            return_agg_func_kwargs=return_agg_func_kwargs,
            price_cols=price_cols,
            price_agg_func=price_agg_func,
            price_agg_func_kwargs=price_agg_func_kwargs,
            volume_cols=volume_cols,
            volume_agg_func=volume_agg_func,
            volume_agg_func_kwargs=volume_agg_func_kwargs,
        )
        return df_out

    @staticmethod
    def _compute_actual_output(df: pd.DataFrame, df_out: pd.DataFrame) -> str:
        act = []
        act.append(hut.convert_df_to_string(df, index=True, title="df"))
        act.append(hut.convert_df_to_string(df_out, index=True, title="df_out"))
        act = "\n".join(act)
        return act


class Test_resample_ohlcv_bars1(hut.TestCase):
    def test1(self) -> None:
        """
        Compute OHLCV bars at the frequency of the data should not change the
        data.
        """
        df = self._get_df()
        rule = "1T"
        df_out = self._helper(df, rule)
        # Compute output.
        act = self._compute_actual_output(df_out)
        self.assert_equal(act, act, fuzzy_match=True)

    def test2(self) -> None:
        """
        Compute OHLCV bars at 5 min frequency on 1 min data.
        """
        df = self._get_df()
        rule = "5T"
        df_out = self._helper(df, rule)
        # Compute output.
        act = self._compute_actual_output(df_out)
        exp = r"""
        df_out
                              open   high    low  close      vol
        datetime
        2016-01-04 09:30:00  95.23  95.23  94.66  94.70  1867590
        2016-01-04 09:35:00  94.72  95.43  94.67  94.97  1776479
        2016-01-04 09:40:00  94.98  96.23  94.95  95.93  2182331
        2016-01-04 09:45:00  95.92  96.13  95.06  95.31  1746253
        2016-01-04 09:50:00  95.31  95.81  95.27  95.61   841564
        """
        self.assert_equal(act, exp, fuzzy_match=True)
        # The first interval corresponds to (9:25, 9:30] and is timestamped with
        # 9:30am. The values are the same as the first row of the input.
        timestamp = "2016-01-04 09:30:00"
        for col in df.columns:
            np.testing.assert_almost_equal(
                df_out.loc[timestamp, col], df.loc[timestamp, col]
            )
        # The second interval corresponds to (9:30, 9:30] and is timestamped with
        # 9:30am. The values are the same as the first row of the input.
        timestamp = "2016-01-04 09:35:00"
        np.testing.assert_almost_equal(df_out.loc[timestamp, "open"], 94.72)
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "high"],
            np.max([95.05, 95.43, 95.39, 95.04, 95.21]),
        )
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "low"],
            np.min([94.67, 94.95, 95.01, 94.75, 94.88]),
        )
        np.testing.assert_almost_equal(df_out.loc[timestamp, "close"], 94.97)
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "vol"],
            np.sum([349119, 419479, 307383, 342218, 358280]),
        )

    def test3(self) -> None:
        """
        Compute OHLCV bars at 1 hr gives first, max, min, last values.
        """
        df = self._get_df()
        rule = "1H"
        df_out = self._helper(df, rule)
        # Compute output.
        act = self._compute_actual_output(df_out)
        exp = r"""
        df_out
                              open   high    low  close      vol
        datetime
        2016-01-04 10:00:00  95.23  96.23  94.66  95.61  8414217
        """
        self.assert_equal(act, exp, fuzzy_match=True)
        # The only interval corresponds to (9:00, 10:00] and is timestamped with
        # 10:00am.
        timestamp = "2016-01-04 10:00:00"
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "open"], df.loc["2016-01-04 09:30:00", "open"]
        )
        df_values = df.drop("vol", axis="columns").values
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "high"], np.max(np.max(df_values, axis=1))
        )
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "low"], np.min(np.min(df_values, axis=1))
        )
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "close"], df.loc["2016-01-04 09:49:00", "close"]
        )
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "vol"], np.sum(df[["vol"]].values)
        )

    @staticmethod
    def _get_df() -> pd.DataFrame:
        """
        Return a df without NaNs.
        """
        # From `s3://*****-data/data/kibot/all_stocks_1min/AAPL.csv.gz`.
        txt = """
datetime,open,high,low,close,vol
2016-01-04 09:30:00,95.23,95.23,94.66,94.7,1867590
2016-01-04 09:31:00,94.72,95.05,94.67,94.98,349119
2016-01-04 09:32:00,94.97,95.43,94.95,95.33,419479
2016-01-04 09:33:00,95.34,95.39,95.01,95.03,307383
2016-01-04 09:34:00,95.01,95.04,94.75,94.89,342218
2016-01-04 09:35:00,94.9,95.21,94.88,94.97,358280
2016-01-04 09:36:00,94.98,95.27,94.95,95.21,266199
2016-01-04 09:37:00,95.21,95.49,95.2,95.48,293074
2016-01-04 09:38:00,95.48,96.04,95.47,95.95,581584
2016-01-04 09:39:00,95.93,96.16,95.82,96.07,554872
2016-01-04 09:40:00,96.08,96.23,95.9,95.93,486602
2016-01-04 09:41:00,95.92,96.13,95.75,95.78,451518
2016-01-04 09:42:00,95.8,95.9,95.59,95.59,315207
2016-01-04 09:43:00,95.61,95.62,95.28,95.29,429025
2016-01-04 09:44:00,95.29,95.41,95.22,95.26,262630
2016-01-04 09:45:00,95.26,95.37,95.06,95.31,287873
2016-01-04 09:46:00,95.31,95.48,95.27,95.41,225975
2016-01-04 09:47:00,95.41,95.71,95.33,95.66,262623
2016-01-04 09:48:00,95.7,95.71,95.5,95.57,179722
2016-01-04 09:49:00,95.56,95.81,95.49,95.61,173244
"""
        df = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        return df

    @staticmethod
    def _helper(df: pd.DataFrame, rule: str) -> pd.DataFrame:
        df_out = fin.resample_ohlcv_bars(
            df,
            rule,
            open_col="open",
            high_col="high",
            low_col="low",
            close_col="close",
            volume_col="vol",
            add_twap_vwap=False,
        )
        return df_out

    @staticmethod
    def _compute_actual_output(df_out: pd.DataFrame) -> str:
        act = []
        act.append(hut.convert_df_to_string(df_out, index=True, title="df_out"))
        act = "\n".join(act)
        return act


class Test_compute_twap_vwap1(hut.TestCase):
    def test_with_no_nans1(self) -> None:
        """
        Compute VWAP/TWAP at the frequency of the data should not change the
        data.
        """
        df = self._get_df_with_no_nans()
        rule = "1T"
        df_out = self._helper(df, rule)
        # Compute output.
        act = self._compute_actual_output(df_out)
        exp = """
        df_out
                              vwap   twap
        datetime
        2016-01-04 09:30:00  94.70  94.70
        2016-01-04 09:31:00  94.98  94.98
        2016-01-04 09:32:00  95.33  95.33
        2016-01-04 09:33:00  95.03  95.03
        2016-01-04 09:34:00  94.89  94.89
        2016-01-04 09:35:00  94.97  94.97
        2016-01-04 09:36:00  95.21  95.21
        2016-01-04 09:37:00  95.48  95.48
        2016-01-04 09:38:00  95.95  95.95
        2016-01-04 09:39:00  96.07  96.07
        2016-01-04 09:40:00  95.93  95.93
        2016-01-04 09:41:00  95.78  95.78
        2016-01-04 09:42:00  95.59  95.59
        2016-01-04 09:43:00  95.29  95.29
        2016-01-04 09:44:00  95.26  95.26
        2016-01-04 09:45:00  95.31  95.31
        2016-01-04 09:46:00  95.41  95.41
        2016-01-04 09:47:00  95.66  95.66
        2016-01-04 09:48:00  95.57  95.57
        2016-01-04 09:49:00  95.61  95.61
        """
        self.assert_equal(act, exp, fuzzy_match=True)
        # TWAP is the same as VWAP aggregating on 1 sample.
        np.testing.assert_array_almost_equal(df_out["vwap"], df_out["twap"])
        # TWAP and VWAP are the same value as the original price aggregating on 1
        # sample.
        np.testing.assert_array_almost_equal(df_out["vwap"], df["close"])

    def test_with_no_nans2(self) -> None:
        """
        Compute VWAP/TWAP at 5 min frequency on 1 min data.
        """
        df = self._get_df_with_no_nans()
        rule = "5T"
        df_out = self._helper(df, rule)
        # Compute output.
        act = self._compute_actual_output(df_out)
        exp = r"""
        df_out
                                  vwap     twap
        datetime
        2016-01-04 09:30:00  94.700000  94.7000
        2016-01-04 09:35:00  95.051943  95.0400
        2016-01-04 09:40:00  95.822669  95.7280
        2016-01-04 09:45:00  95.469633  95.4460
        2016-01-04 09:50:00  95.563357  95.5625
        """
        self.assert_equal(act, exp, fuzzy_match=True)
        # The first interval is (9:25, 9:30] so VWAP/TWAP matches the original data.
        timestamp = "2016-01-04 09:30:00"
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "vwap"], df.loc[timestamp, "close"]
        )
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "twap"], df.loc[timestamp, "close"]
        )
        # The second interval is (9:30, 9:35].
        timestamp = "2016-01-04 09:35:00"
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "vwap"],
            (
                94.98 * 349119
                + 95.33 * 419479
                + 95.03 * 307383
                + 94.89 * 342218
                + 94.97 * 358280
            )
            / (349119 + 419479 + 307383 + 342218 + 358280),
        )
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "twap"],
            np.mean([94.98, 95.33, 95.03, 94.89, 94.97]),
        )
        # The last interval is (9:45, 9:50].
        timestamp = "2016-01-04 09:50:00"
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "vwap"],
            (95.41 * 225975 + 95.66 * 262623 + 95.57 * 179722 + 95.61 * 173244)
            / (225975 + 262623 + 179722 + 173244),
        )
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "twap"], np.mean([95.41, 95.66, 95.57, 95.61])
        )

    def test_with_nans1(self) -> None:
        """
        Compute VWAP/TWAP at the frequency of the data.
        """
        df = self._get_df_with_nans()
        rule = "1T"
        df_out = self._helper(df, rule)
        # Compute output.
        act = self._compute_actual_output(df_out)
        exp = """
        df_out
                              vwap   twap
        datetime
        2016-01-04 09:30:00    NaN    NaN
        2016-01-04 09:31:00    NaN    NaN
        2016-01-04 09:32:00  95.33  95.33
        2016-01-04 09:33:00  95.03  95.03
        2016-01-04 09:34:00  94.89  94.89
        2016-01-04 09:35:00  94.97  94.97
        2016-01-04 09:36:00    NaN    NaN
        2016-01-04 09:37:00    NaN    NaN
        2016-01-04 09:38:00  95.95  95.95
        2016-01-04 09:39:00  96.07  96.07
        2016-01-04 09:40:00    NaN    NaN
        2016-01-04 09:41:00    NaN    NaN
        2016-01-04 09:42:00    NaN    NaN
        2016-01-04 09:43:00    NaN    NaN
        2016-01-04 09:44:00    NaN    NaN
        2016-01-04 09:45:00    NaN    NaN
        2016-01-04 09:46:00  95.41  95.41
        2016-01-04 09:47:00  95.66  95.66
        2016-01-04 09:48:00  95.57  95.57
        2016-01-04 09:49:00  95.61  95.61
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_with_nans2(self) -> None:
        """
        Compute VWAP/TWAP at 5 min frequency on 1 min data.
        """
        # TODO(*): Put the relevant data close to the test.
        df = self._get_df_with_nans()
        rule = "5T"
        df_out = self._helper(df, rule)
        # Compute output.
        act = self._compute_actual_output(df_out)
        exp = r"""
        df_out
                                  vwap     twap
        datetime
        2016-01-04 09:30:00        NaN      NaN
        2016-01-04 09:35:00  95.069539  95.0550
        2016-01-04 09:40:00  96.008590  96.0100
        2016-01-04 09:45:00        NaN      NaN
        2016-01-04 09:50:00  95.563357  95.5625
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_with_offset(self) -> None:
        """
        Compute VWAP/TWAP at 5 min frequency on 1 min data.
        """
        df = self._get_df_with_no_nans()
        rule = "5T"
        offset = "1T"
        df_out = self._helper(df, rule, offset)
        # Compute output.
        act = self._compute_actual_output(df_out)
        exp = r"""
        df_out
                                  vwap       twap
        datetime
        2016-01-04 09:31:00  94.744098  94.840000
        2016-01-04 09:36:00  95.091617  95.086000
        2016-01-04 09:41:00  95.883415  95.842000
        2016-01-04 09:46:00  95.368620  95.372000
        2016-01-04 09:51:00  95.619653  95.613333
        """
        self.assert_equal(act, exp, fuzzy_match=True)
        # The first interval is (9:26, 9:31].
        timestamp = "2016-01-04 09:31:00"
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "vwap"],
            (94.7 * 1867590 + 94.98 * 349119) / (1867590 + 349119),
        )
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "twap"],
            np.mean([94.7, 94.98]),
        )
        # The second interval is (9:31, 9:36].
        timestamp = "2016-01-04 09:36:00"
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "vwap"],
            (
                95.33 * 419479
                + 95.03 * 307383
                + 94.89 * 342218
                + 94.97 * 358280
                + 95.21 * 266199
            )
            / (419479 + 307383 + 342218 + 358280 + 266199),
        )
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "twap"],
            np.mean([95.33, 95.03, 94.89, 94.97, 95.21]),
        )
        # The last interval is (9:46, 9:51].
        timestamp = "2016-01-04 09:51:00"
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "vwap"],
            (95.66 * 262623 + 95.57 * 179722 + 95.61 * 173244)
            / (262623 + 179722 + 173244),
        )
        np.testing.assert_almost_equal(
            df_out.loc[timestamp, "twap"], np.mean([95.66, 95.57, 95.61])
        )

    @staticmethod
    def _get_df_with_no_nans() -> pd.DataFrame:
        """
        Return a df without NaNs.
        """
        # From `s3://*****-data/data/kibot/all_stocks_1min/AAPL.csv.gz`.
        txt = """
datetime,close,vol
2016-01-04 09:30:00,94.7,1867590
2016-01-04 09:31:00,94.98,349119
2016-01-04 09:32:00,95.33,419479
2016-01-04 09:33:00,95.03,307383
2016-01-04 09:34:00,94.89,342218
2016-01-04 09:35:00,94.97,358280
2016-01-04 09:36:00,95.21,266199
2016-01-04 09:37:00,95.48,293074
2016-01-04 09:38:00,95.95,581584
2016-01-04 09:39:00,96.07,554872
2016-01-04 09:40:00,95.93,486602
2016-01-04 09:41:00,95.78,451518
2016-01-04 09:42:00,95.59,315207
2016-01-04 09:43:00,95.29,429025
2016-01-04 09:44:00,95.26,262630
2016-01-04 09:45:00,95.31,287873
2016-01-04 09:46:00,95.41,225975
2016-01-04 09:47:00,95.66,262623
2016-01-04 09:48:00,95.57,179722
2016-01-04 09:49:00,95.61,173244
"""
        df = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        return df

    @staticmethod
    def _get_df_with_nans() -> pd.DataFrame:
        """
        Compute a df with NaNs.
        """
        # From `s3://*****-data/data/kibot/all_stocks_1min/AAPL.csv.gz`.
        txt = """
datetime,close,vol
2016-01-04 09:30:00,NaN,1867590
2016-01-04 09:31:00,94.98,NaN
2016-01-04 09:32:00,95.33,419479
2016-01-04 09:33:00,95.03,307383
2016-01-04 09:34:00,94.89,342218
2016-01-04 09:35:00,94.97,358280
2016-01-04 09:36:00,NaN,266199
2016-01-04 09:37:00,95.48,NaN
2016-01-04 09:38:00,95.95,581584
2016-01-04 09:39:00,96.07,554872
2016-01-04 09:40:00,NaN,NaN
2016-01-04 09:41:00,NaN,451518
2016-01-04 09:42:00,NaN,315207
2016-01-04 09:43:00,NaN,429025
2016-01-04 09:44:00,NaN,262630
2016-01-04 09:45:00,NaN,287873
2016-01-04 09:46:00,95.41,225975
2016-01-04 09:47:00,95.66,262623
2016-01-04 09:48:00,95.57,179722
2016-01-04 09:49:00,95.61,173244
"""
        df = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        return df

    @staticmethod
    def _helper(
        df: pd.DataFrame, rule: str, offset: Optional[str] = None
    ) -> pd.DataFrame:
        price_col = "close"
        volume_col = "vol"
        df_out = fin.compute_twap_vwap(
            df,
            rule,
            price_col=price_col,
            volume_col=volume_col,
            offset=offset,
        )
        return df_out

    @staticmethod
    def _compute_actual_output(df_out: pd.DataFrame) -> str:
        act = []
        act.append(hut.convert_df_to_string(df_out, index=True, title="df_out"))
        act = "\n".join(act)
        return act


class Test_process_bid_ask(hut.TestCase):
    def test_mid(self) -> None:
        df = self._get_df()
        actual = fin.process_bid_ask(
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
        actual = fin.process_bid_ask(
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
        actual = fin.process_bid_ask(
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
        actual = fin.process_bid_ask(
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
        actual = fin.process_bid_ask(
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
        actual = fin.process_bid_ask(
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
        actual = fin.process_bid_ask(
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

    def test_bid_value(self) -> None:
        df = self._get_df()
        actual = fin.process_bid_ask(
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
        actual = fin.process_bid_ask(
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
        actual = fin.process_bid_ask(
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
        """
        Return a df without NaNs.
        """
        # From `s3://*****-data/data/kibot/all_stocks_1min/AAPL.csv.gz`.
        txt = """
datetime,bid,ask,bid_volume,ask_volume
2016-01-04 12:00:00,100.01,100.02,200,200
2016-01-04 12:01:00,100.01,100.02,200,300
2016-01-04 12:02:00,99.99,100.01,300,300
2016-01-04 12:03:00,99.98,100.02,200,400
"""
        df = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        return df


class Test_process_bid_ask(hut.TestCase):
    def test_mid(self) -> None:
        df = self._get_df()
        actual = fin.process_bid_ask(
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
        actual = fin.process_bid_ask(
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
        actual = fin.process_bid_ask(
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
        actual = fin.process_bid_ask(
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
        actual = fin.process_bid_ask(
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
        actual = fin.process_bid_ask(
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
        actual = fin.process_bid_ask(
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

    def test_bid_value(self) -> None:
        df = self._get_df()
        actual = fin.process_bid_ask(
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
        actual = fin.process_bid_ask(
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
        actual = fin.process_bid_ask(
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


class Test_compute_spread_cost(hut.TestCase):
    def test_one_half_spread(self) -> None:
        df = self._get_df()
        actual = fin.compute_spread_cost(
            df,
            target_position_col="position",
            spread_col="spread",
            spread_fraction_paid=0.5,
        )
        txt = """
datetime,spread_cost
2016-01-04 12:00:00,NaN
2016-01-04 12:01:00,NaN
2016-01-04 12:02:00,0.01
2016-01-04 12:03:00,0.06
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        np.testing.assert_allclose(actual, expected)

    def test_one_third_spread(self) -> None:
        df = self._get_df()
        actual = fin.compute_spread_cost(
            df,
            target_position_col="position",
            spread_col="spread",
            spread_fraction_paid=0.33,
        )
        txt = """
datetime,spread_cost
2016-01-04 12:00:00,NaN
2016-01-04 12:01:00,NaN
2016-01-04 12:02:00,0.0066
2016-01-04 12:03:00,0.0396
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        np.testing.assert_allclose(actual, expected)

    @staticmethod
    def _get_df() -> pd.DataFrame:
        txt = """
datetime,spread,position
2016-01-04 12:00:00,0.0001,100
2016-01-04 12:01:00,0.0001,200
2016-01-04 12:02:00,0.0002,-100
2016-01-04 12:03:00,0.0004,100
"""
        df = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        return df


class Test_compute_pnl(hut.TestCase):
    def test1(self) -> None:
        df = self._get_df()
        actual = fin.compute_pnl(
            df,
            position_intent_col="position_intent_1",
            return_col="ret_0",
        )
        txt = """
datetime,pnl
2016-01-04 12:00:00,NaN
2016-01-04 12:01:00,NaN
2016-01-04 12:02:00,0.06
2016-01-04 12:03:00,0.24
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        np.testing.assert_allclose(actual, expected)

    @staticmethod
    def _get_df() -> pd.DataFrame:
        txt = """
datetime,ret_0,position_intent_1
2016-01-04 12:00:00,0.0010,100
2016-01-04 12:01:00,-0.0008,200
2016-01-04 12:02:00,0.0006,-100
2016-01-04 12:03:00,0.0012,100
"""
        df = pd.read_csv(io.StringIO(txt), index_col=0, parse_dates=True)
        return df


class Test_compute_inverse_volatility_weights(hut.TestCase):
    def test1(self) -> None:
        """
        Test for a clean input.
        """
        sample = self._get_sample(seed=1)
        weights = fin.compute_inverse_volatility_weights(sample)
        output_txt = self._get_output_txt(sample, weights)
        self.check_string(output_txt)

    def test2(self) -> None:
        """
        Test for an input with NaNs.
        """
        sample = self._get_sample(seed=1)
        sample.iloc[1, 1] = np.nan
        sample.iloc[0:5, 0] = np.nan
        weights = fin.compute_inverse_volatility_weights(sample)
        output_txt = self._get_output_txt(sample, weights)
        self.check_string(output_txt)

    def test3(self) -> None:
        """
        Test for an input with all-NaN column.

        Results are not intended. `weights` are `0` for all-NaN columns
        in the input.
        """
        sample = self._get_sample(seed=1)
        sample.iloc[:, 0] = np.nan
        weights = fin.compute_inverse_volatility_weights(sample)
        output_txt = self._get_output_txt(sample, weights)
        self.check_string(output_txt)

    def test4(self) -> None:
        """
        Test for an all-NaN input.

        Results are not intended. `weights` are `0` for all-NaN columns
        in the input.
        """
        sample = self._get_sample(seed=1)
        sample.iloc[:, :] = np.nan
        weights = fin.compute_inverse_volatility_weights(sample)
        output_txt = self._get_output_txt(sample, weights)
        self.check_string(output_txt)

    @staticmethod
    def _get_sample(seed: int) -> pd.DataFrame:
        mean = pd.Series([1, 2])
        cov = pd.DataFrame([[0.5, 0.2], [0.2, 0.3]])
        date_range = {"start": "2010-01-01", "periods": 40, "freq": "B"}
        mn_process = sig_gen.MultivariateNormalProcess(mean=mean, cov=cov)
        sample = mn_process.generate_sample(date_range, seed=seed)
        sample.rename(columns={0: "srs1", 1: "srs2"}, inplace=True)
        return sample

    @staticmethod
    def _get_output_txt(sample: pd.DataFrame, weights: pd.Series) -> str:
        sample_string = hut.convert_df_to_string(sample, index=True)
        weights_string = hut.convert_df_to_string(weights, index=True)
        txt = (
            f"Input sample:\n{sample_string}\n\n"
            f"Output weights:\n{weights_string}\n"
        )
        return txt


class Test_compute_prices_from_rets(hut.TestCase):
    def test1(self) -> None:
        sample = self._get_sample()
        sample["rets"] = fin.compute_ret_0(sample.price, mode="pct_change")
        sample["price_pred"] = fin.compute_prices_from_rets(
            sample.price, sample.rets, "pct_change"
        )
        sample = sample.dropna()
        np.testing.assert_array_almost_equal(sample.price_pred, sample.price)

    def test2(self) -> None:
        sample = self._get_sample()
        sample["rets"] = fin.compute_ret_0(sample.price, mode="log_rets")
        sample["price_pred"] = fin.compute_prices_from_rets(
            sample.price, sample.rets, "log_rets"
        )
        sample = sample.dropna()
        np.testing.assert_array_almost_equal(sample.price_pred, sample.price)

    def test3(self) -> None:
        sample = self._get_sample()
        sample["rets"] = fin.compute_ret_0(sample.price, mode="diff")
        sample["price_pred"] = fin.compute_prices_from_rets(
            sample.price, sample.rets, "diff"
        )
        sample = sample.dropna()
        np.testing.assert_array_almost_equal(sample.price_pred, sample.price)

    def test4(self) -> None:
        """
        Check prices from forward returns.
        """
        sample = pd.DataFrame({"price": [1, 2, 3], "fwd_ret": [1, 0.5, np.nan]})
        sample["ret_0"] = sample.fwd_ret.shift(1)
        sample["price_pred"] = fin.compute_prices_from_rets(
            sample.price, sample.ret_0, "pct_change"
        ).shift(1)
        sample = sample.dropna()
        np.testing.assert_array_almost_equal(sample.price_pred, sample.price)

    def test5(self) -> None:
        """
        Check output with forward returns.
        """
        np.random.seed(0)
        sample = self._get_sample()
        sample["ret_0"] = fin.compute_ret_0(sample.price, mode="log_rets")
        sample["ret_1"] = sample["ret_0"].shift(-1)
        sample["price_pred"] = fin.compute_prices_from_rets(
            sample.price, sample.ret_1.shift(1), "log_rets"
        )
        output_txt = hut.convert_df_to_string(sample, index=True)
        self.check_string(output_txt)

    def test6(self) -> None:
        """
        Check future price prediction.
        """
        np.random.seed(1)
        sample = self._get_sample()
        sample["ret_1"] = fin.compute_ret_0(sample.price, mode="log_rets").shift(
            -1
        )
        future_price_expected = sample.iloc[-1, 0]
        # Drop latest date price.
        sample.dropna(inplace=True)
        rets = sample["ret_1"]
        rets.index = rets.index.shift(1)
        # Make future prediction for the dropped price.
        future_price_actual = fin.compute_prices_from_rets(
            sample.price, rets, "log_rets"
        )[-1]
        np.testing.assert_almost_equal(future_price_expected, future_price_actual)

    @staticmethod
    def _get_sample() -> pd.DataFrame:
        date_range = pd.date_range(start="2010-01-01", periods=40, freq="B")
        sample = pd.DataFrame(index=date_range)
        sample["price"] = np.random.uniform(low=0, high=1, size=40)
        return sample


class Test_aggregate_log_rets(hut.TestCase):
    def test1(self) -> None:
        """
        Test for a clean input.
        """
        sample = self._get_sample(seed=1)
        weights = fin.compute_inverse_volatility_weights(sample)
        aggregate_log_rets = fin.aggregate_log_rets(sample, weights)
        output_txt = self._get_output_txt(sample, weights, aggregate_log_rets)
        self.check_string(output_txt)

    def test2(self) -> None:
        """
        Test for an input with NaNs.
        """
        sample = self._get_sample(seed=1)
        sample.iloc[1, 1] = np.nan
        sample.iloc[0:5, 0] = np.nan
        weights = fin.compute_inverse_volatility_weights(sample)
        aggregate_log_rets = fin.aggregate_log_rets(sample, weights)
        output_txt = self._get_output_txt(sample, weights, aggregate_log_rets)
        self.check_string(output_txt)

    def test3(self) -> None:
        """
        Test for an input with all-NaN column.
        """
        sample = self._get_sample(seed=1)
        sample.iloc[:, 0] = np.nan
        weights = pd.Series([0.5, 0.5], index=["srs1", "srs2"], name="weights")
        aggregate_log_rets = fin.aggregate_log_rets(sample, weights)
        output_txt = self._get_output_txt(sample, weights, aggregate_log_rets)
        self.check_string(output_txt)

    def test4(self) -> None:
        """
        Test for an all-NaN input.
        """
        sample = self._get_sample(seed=1)
        sample.iloc[:, :] = np.nan
        weights = pd.Series([0.5, 0.5], index=["srs1", "srs2"], name="weights")
        aggregate_log_rets = fin.aggregate_log_rets(sample, weights)
        output_txt = self._get_output_txt(sample, weights, aggregate_log_rets)
        self.check_string(output_txt)

    @staticmethod
    def _get_sample(seed: int) -> pd.DataFrame:
        mean = pd.Series([1, 2])
        cov = pd.DataFrame([[0.5, 0.2], [0.2, 0.3]])
        date_range = {"start": "2010-01-01", "periods": 40, "freq": "B"}
        mn_process = sig_gen.MultivariateNormalProcess(mean=mean, cov=cov)
        sample = mn_process.generate_sample(date_range, seed=seed)
        sample.rename(columns={0: "srs1", 1: "srs2"}, inplace=True)
        return sample

    @staticmethod
    def _get_output_txt(
        sample: pd.DataFrame, weights: pd.Series, aggregate_log_rets: pd.Series
    ) -> str:
        sample_string = hut.convert_df_to_string(sample, index=True)
        weights_string = hut.convert_df_to_string(weights, index=True)
        aggregate_log_rets_string = hut.convert_df_to_string(
            aggregate_log_rets, index=True
        )
        txt = (
            f"Input sample:\n{sample_string}\n\n"
            f"Input weights:\n{weights_string}\n\n"
            f"Output aggregate log returns:\n{aggregate_log_rets_string}\n"
        )
        return txt


class Test_compute_kratio(hut.TestCase):
    def test1(self) -> None:
        """
        Test for an clean input series.
        """
        series = self._get_series(seed=1)
        actual = fin.compute_kratio(series)
        expected = -0.84551
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test2(self) -> None:
        """
        Test for an input with NaN values.
        """
        series = self._get_series(seed=1)
        series[:3] = np.nan
        series[7:10] = np.nan
        actual = fin.compute_kratio(series)
        expected = -0.85089
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series


class Test_compute_drawdown(hut.TestCase):
    def test1(self) -> None:
        series = self._get_series(1)
        actual = fin.compute_drawdown(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series


class Test_compute_time_under_water(hut.TestCase):
    def test1(self) -> None:
        series = Test_compute_time_under_water._get_series(42)
        drawdown = fin.compute_drawdown(series).rename("drawdown")
        time_under_water = fin.compute_time_under_water(series).rename(
            "time_under_water"
        )
        output = pd.concat([series, drawdown, time_under_water], axis=1)
        self.check_string(hut.convert_df_to_string(output, index=True))

    def test2(self) -> None:
        series = Test_compute_time_under_water._get_series(42)
        series.iloc[:4] = np.nan
        series.iloc[10:15] = np.nan
        series.iloc[-4:] = np.nan
        drawdown = fin.compute_drawdown(series).rename("drawdown")
        time_under_water = fin.compute_time_under_water(series).rename(
            "time_under_water"
        )
        output = pd.concat([series, drawdown, time_under_water], axis=1)
        self.check_string(hut.convert_df_to_string(output, index=True))

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arma_process = sig_gen.ArmaProcess([], [])
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series


class Test_compute_turnover(hut.TestCase):
    def test1(self) -> None:
        """
        Test for default arguments.
        """
        series = self._get_series(seed=1)
        series[5:10] = np.nan
        actual = fin.compute_turnover(series).rename("output")
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test2(self) -> None:
        """
        Test for only positive input.
        """
        positive_series = self._get_series(seed=1).abs()
        actual = fin.compute_turnover(positive_series).rename("output")
        output_df = pd.concat([positive_series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test3(self) -> None:
        """
        Test for nan_mode.
        """
        series = self._get_series(seed=1)
        series[5:10] = np.nan
        actual = fin.compute_turnover(series, nan_mode="ffill").rename("output")
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test4(self) -> None:
        """
        Test for unit.
        """
        series = self._get_series(seed=1)
        series[5:10] = np.nan
        actual = fin.compute_turnover(series, unit="B").rename("output")
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "D"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        ).rename("input")
        return series


class Test_compute_average_holding_period(hut.TestCase):
    def test1(self) -> None:
        series = self._get_series_in_unit(seed=1)
        series[5:10] = np.nan
        actual = fin.compute_average_holding_period(series)
        expected = 1.08458
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test2(self) -> None:
        positive_series = self._get_series_in_unit(seed=1).abs()
        actual = fin.compute_average_holding_period(positive_series)
        expected = 1.23620
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test3(self) -> None:
        series = self._get_series_in_unit(seed=1)
        actual = fin.compute_average_holding_period(series, unit="M")
        expected = 0.05001
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    @staticmethod
    def _get_series_in_unit(seed: int, freq: str = "D") -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": freq}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series


class Test_compute_bet_starts(hut.TestCase):
    def test1(self) -> None:
        positions = Test_compute_bet_starts._get_series(42)
        actual = fin.compute_bet_starts(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_lengths')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test2(self) -> None:
        positions = Test_compute_bet_starts._get_series(42)
        positions.iloc[:4] = np.nan
        positions.iloc[10:15] = np.nan
        positions.iloc[-4:] = np.nan
        actual = fin.compute_bet_starts(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_lengths')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test3(self) -> None:
        """
        Test zeros.
        """
        positions = pd.Series(
            {
                pd.Timestamp("2010-01-01"): 0,
                pd.Timestamp("2010-01-02"): 1,
                pd.Timestamp("2010-01-03"): 0,
                pd.Timestamp("2010-01-04"): -1,
            }
        )
        expected = pd.Series(
            {
                pd.Timestamp("2010-01-01"): np.nan,
                pd.Timestamp("2010-01-02"): 1,
                pd.Timestamp("2010-01-03"): np.nan,
                pd.Timestamp("2010-01-04"): -1,
            },
            dtype=float,
        )
        actual = fin.compute_bet_starts(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test4(self) -> None:
        """
        Test `NaN`s.
        """
        positions = pd.Series(
            {
                pd.Timestamp("2010-01-01"): np.nan,
                pd.Timestamp("2010-01-02"): 1,
                pd.Timestamp("2010-01-03"): np.nan,
                pd.Timestamp("2010-01-04"): -1,
            }
        )
        expected = pd.Series(
            {
                pd.Timestamp("2010-01-01"): np.nan,
                pd.Timestamp("2010-01-02"): 1,
                pd.Timestamp("2010-01-03"): np.nan,
                pd.Timestamp("2010-01-04"): -1,
            },
            dtype=float,
        )
        actual = fin.compute_bet_starts(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test5(self) -> None:
        """
        Test consecutive zeroes.
        """
        positions = pd.Series(
            {
                pd.Timestamp("2010-01-01"): 0,
                pd.Timestamp("2010-01-02"): 0,
                pd.Timestamp("2010-01-03"): 0,
                pd.Timestamp("2010-01-04"): -1,
            }
        )
        expected = pd.Series(
            {
                pd.Timestamp("2010-01-01"): np.nan,
                pd.Timestamp("2010-01-02"): np.nan,
                pd.Timestamp("2010-01-03"): np.nan,
                pd.Timestamp("2010-01-04"): -1,
            },
            dtype=float,
        )
        actual = fin.compute_bet_starts(positions)
        pd.testing.assert_series_equal(actual, expected)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series


class Test_compute_bet_ends(hut.TestCase):
    def test1(self) -> None:
        positions = Test_compute_bet_ends._get_series(42)
        actual = fin.compute_bet_ends(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_lengths')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test2(self) -> None:
        positions = Test_compute_bet_ends._get_series(42)
        positions.iloc[:4] = np.nan
        positions.iloc[10:15] = np.nan
        positions.iloc[-4:] = np.nan
        actual = fin.compute_bet_ends(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_lengths')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test3(self) -> None:
        """
        Test zeros.
        """
        positions = pd.Series(
            {
                pd.Timestamp("2010-01-01"): 0,
                pd.Timestamp("2010-01-02"): 1,
                pd.Timestamp("2010-01-03"): 0,
                pd.Timestamp("2010-01-04"): -1,
            }
        )
        expected = pd.Series(
            {
                pd.Timestamp("2010-01-01"): np.nan,
                pd.Timestamp("2010-01-02"): 1,
                pd.Timestamp("2010-01-03"): np.nan,
                pd.Timestamp("2010-01-04"): -1,
            },
            dtype=float,
        )
        # TODO(*): This is testing the wrong function!
        actual = fin.compute_bet_starts(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test4(self) -> None:
        """
        Test `NaN`s.
        """
        positions = pd.Series(
            {
                pd.Timestamp("2010-01-01"): np.nan,
                pd.Timestamp("2010-01-02"): 1,
                pd.Timestamp("2010-01-03"): np.nan,
                pd.Timestamp("2010-01-04"): -1,
            }
        )
        expected = pd.Series(
            {
                pd.Timestamp("2010-01-01"): np.nan,
                pd.Timestamp("2010-01-02"): 1,
                pd.Timestamp("2010-01-03"): np.nan,
                pd.Timestamp("2010-01-04"): -1,
            },
            dtype=float,
        )
        # TODO(*): This is testing the wrong function!
        actual = fin.compute_bet_starts(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test5(self) -> None:
        """
        Test consecutive zeroes.
        """
        positions = pd.Series(
            {
                pd.Timestamp("2010-01-01"): -1,
                pd.Timestamp("2010-01-02"): 0,
                pd.Timestamp("2010-01-03"): 0,
                pd.Timestamp("2010-01-04"): 0,
            }
        )
        expected = pd.Series(
            {
                pd.Timestamp("2010-01-01"): -1,
                pd.Timestamp("2010-01-02"): np.nan,
                pd.Timestamp("2010-01-03"): np.nan,
                pd.Timestamp("2010-01-04"): np.nan,
            },
            dtype=float,
        )
        # TODO(*): This is testing the wrong function!
        actual = fin.compute_bet_starts(positions)
        pd.testing.assert_series_equal(actual, expected)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series


class Test_compute_signed_bet_lengths(hut.TestCase):
    def test1(self) -> None:
        positions = Test_compute_signed_bet_lengths._get_series(42)
        actual = fin.compute_signed_bet_lengths(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_lengths')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test2(self) -> None:
        positions = Test_compute_signed_bet_lengths._get_series(42)
        positions.iloc[:4] = np.nan
        positions.iloc[10:15] = np.nan
        positions.iloc[-4:] = np.nan
        actual = fin.compute_signed_bet_lengths(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_lengths')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test3(self) -> None:
        positions = pd.Series(
            [1, 1, 1, 2, -1, -4, -0.5, 0, 0, -1, 0, 1],
            index=pd.date_range(start="2010-01-01", periods=12, freq="D"),
        )
        expected_bet_ends = pd.to_datetime(
            ["2010-01-04", "2010-01-07", "2010-01-10", "2010-01-12"]
        )
        expected = pd.Series([4, -3, -1, 1], index=expected_bet_ends, dtype=float)
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test4(self) -> None:
        """
        Test a single value.
        """
        positions = pd.Series([1], index=[pd.Timestamp("2010-01-01")])
        # Notice the int to float data type change.
        expected = pd.Series([1], index=[pd.Timestamp("2010-01-01")], dtype=float)
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test5(self) -> None:
        """
        Test NaNs.
        """
        idx = pd.to_datetime(["2010-01-01", "2010-01-02"])
        positions = pd.Series([np.nan, np.nan], index=idx)
        expected = pd.Series(index=idx).dropna()
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test6(self) -> None:
        """
        Test NaNs.
        """
        idx = pd.to_datetime(["2010-01-01", "2010-01-02", "2010-01-03"])
        positions = pd.Series([1, np.nan, 0], index=idx)
        expected = pd.Series(
            [1], index=pd.to_datetime(["2010-01-01"]), dtype=float
        )
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test7(self) -> None:
        """
        Test NaNs.
        """
        idx = pd.to_datetime(["2010-01-01", "2010-01-02", "2010-01-03"])
        positions = pd.Series([1, np.nan, np.nan], index=idx)
        expected = pd.Series(
            [1], index=pd.to_datetime(["2010-01-01"]), dtype=float
        )
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test8(self) -> None:
        """
        Test zeroes.
        """
        idx = pd.to_datetime(["2010-01-01", "2010-01-02"])
        positions = pd.Series([0, 0], index=idx)
        expected = pd.Series(index=idx).dropna()
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test9(self) -> None:
        """
        Test zeroes.
        """
        idx = pd.to_datetime(["2010-01-01", "2010-01-02", "2010-01-03"])
        positions = pd.Series([0, 1, 0], index=idx)
        expected = pd.Series([1.0], index=[pd.Timestamp("2010-01-02")])
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test10(self) -> None:
        """
        Test zeroes.
        """
        idx = pd.to_datetime(["2010-01-01", "2010-01-02", "2010-01-03"])
        positions = pd.Series([1, 0, 0], index=idx)
        expected = pd.Series([1.0], index=[pd.Timestamp("2010-01-01")])
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test11(self) -> None:
        positions = Test_compute_signed_bet_lengths._get_series(42)
        positions.iloc[:4] = 0
        positions.iloc[10:15] = 0
        positions.iloc[-4:] = 0
        actual = fin.compute_signed_bet_lengths(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_lengths')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test12(self) -> None:
        positions = Test_compute_signed_bet_lengths._get_series(42)
        positions.iloc[:4] = 0
        positions.iloc[10:15] = 0
        positions.iloc[-4:] = 0
        actual = fin.compute_signed_bet_lengths(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_lengths')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series


class Test_compute_returns_per_bet(hut.TestCase):
    def test1(self) -> None:
        """
        Test for clean input series.
        """
        log_rets = self._get_series(42)
        positions = sigp.compute_smooth_moving_average(log_rets, 4)
        actual = fin.compute_returns_per_bet(positions, log_rets)
        rets_pos = pd.concat({"pos": positions, "rets": log_rets}, axis=1)
        output_str = (
            f"{prnt.frame('rets_pos')}\n"
            f"{hut.convert_df_to_string(rets_pos, index=True)}\n"
            f"{prnt.frame('rets_per_bet')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test2(self) -> None:
        """
        Test for input series with NaNs and zeros.
        """
        log_rets = self._get_series(42)
        log_rets.iloc[6:12] = np.nan
        positions = sigp.compute_smooth_moving_average(log_rets, 4)
        positions.iloc[:4] = 0
        positions.iloc[10:15] = np.nan
        positions.iloc[-4:] = 0
        actual = fin.compute_returns_per_bet(positions, log_rets)
        rets_pos = pd.concat({"pos": positions, "rets": log_rets}, axis=1)
        output_str = (
            f"{prnt.frame('rets_pos')}\n"
            f"{hut.convert_df_to_string(rets_pos, index=True)}\n"
            f"{prnt.frame('rets_per_bet')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test3(self) -> None:
        """
        Test for short input series.
        """
        idx = pd.to_datetime(
            [
                "2010-01-01",
                "2010-01-03",
                "2010-01-05",
                "2010-01-06",
                "2010-01-10",
                "2010-01-12",
            ]
        )
        log_rets = pd.Series([1.0, 2.0, 3.0, 5.0, 7.0, 11.0], index=idx)
        positions = pd.Series([1.0, 2.0, 0.0, 1.0, -3.0, -2.0], index=idx)
        actual = fin.compute_returns_per_bet(positions, log_rets)
        expected = pd.Series(
            {
                pd.Timestamp("2010-01-03"): 5.0,
                pd.Timestamp("2010-01-06"): 5.0,
                pd.Timestamp("2010-01-12"): -43.0,
            }
        )
        pd.testing.assert_series_equal(actual, expected)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arma_process = sig_gen.ArmaProcess([], [])
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series
