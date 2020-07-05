import logging

import numpy as np
import pandas as pd

import core.artificial_signal_generators as sig_gen
import core.finance as fin
import helpers.printing as prnt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_aggregate_log_rets(hut.TestCase):
    @staticmethod
    def _get_sample(seed: int) -> pd.DataFrame:
        mean = pd.Series([1, 2])
        cov = pd.DataFrame([[0.5, 0.2], [0.2, 0.3]])
        date_range = {"start": "2010-01-01", "periods": 40, "freq": "B"}
        mn_process = sig_gen.MultivariateNormalProcess(mean=mean, cov=cov)
        sample = mn_process.generate_sample(date_range, seed=seed)
        return sample

    def test1(self) -> None:
        sample = self._get_sample(42)
        actual = fin.aggregate_log_rets(sample, 0.1)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)


class Test_compute_drawdown(hut.TestCase):
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

    def test1(self) -> None:
        series = self._get_series(1)
        actual = fin.compute_drawdown(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)


class Test_compute_turnover(hut.TestCase):
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

    def test1(self) -> None:
        series = self._get_series(seed=1)
        series[5:10] = np.nan
        actual = fin.compute_turnover(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        positive_series = self._get_series(seed=1).abs()
        actual = fin.compute_turnover(positive_series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_series(seed=1)
        series[5:10] = np.nan
        actual = fin.compute_turnover(series, nan_mode="fill_with_zero")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)


class Test_compute_average_holding_period(hut.TestCase):
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

    def test1(self) -> None:
        series = self._get_series_in_unit(seed=1)
        series[5:10] = np.nan
        actual = fin.compute_average_holding_period(series)
        expected = 1.23458
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


class Test_compute_bet_runs(hut.TestCase):
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

    def test1(self) -> None:
        positions = Test_compute_bet_runs._get_series(42)
        actual = fin.compute_bet_runs(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_runs')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test2(self) -> None:
        positions = Test_compute_bet_runs._get_series(42)
        positions.iloc[:4] = np.nan
        positions.iloc[10:15] = np.nan
        positions.iloc[-4:] = np.nan
        actual = fin.compute_bet_runs(positions)
        output_str = (
            f"{prnt.frame('positions')}\n"
            f"{hut.convert_df_to_string(positions, index=True)}\n"
            f"{prnt.frame('bet_runs')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test3(self) -> None:
        """
        Test zeros.
        """
        idx = pd.date_range(start="2010-01-01", periods=4, freq="D")
        positions = pd.Series([0, 0, 0, 1], index=idx)
        expected = pd.Series([0, 0, 0, 1], index=idx, dtype=float)
        actual = fin.compute_bet_runs(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test4(self) -> None:
        """
        Test all zeros.
        """
        idx = pd.date_range(start="2010-01-01", periods=2, freq="D")
        positions = pd.Series([0, 0], index=idx)
        expected = pd.Series([0, 0], index=idx, dtype=float)
        actual = fin.compute_bet_runs(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test5(self) -> None:
        """
        Test NaNs.
        """
        idx = pd.date_range(start="2010-01-01", periods=4, freq="D")
        positions = pd.Series([np.nan, np.nan, 0, 1], index=idx)
        expected = pd.Series([np.nan, np.nan, 0, 1], index=idx, dtype=float)
        actual = fin.compute_bet_runs(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test6(self) -> None:
        """
        Test series with a single value.
        """
        positions = pd.Series([1], index=[pd.Timestamp("2010-01-01")])
        expected = pd.Series([1], index=[pd.Timestamp("2010-01-01")], dtype=float)
        actual = fin.compute_bet_runs(positions)
        pd.testing.assert_series_equal(actual, expected)


class Test_compute_signed_bet_lengths(hut.TestCase):
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

    def test1(self) -> None:
        positions = Test_compute_signed_bet_lengths._get_series(42)
        actual = fin.compute_signed_bet_lengths(positions, "fill_with_zero")
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
        actual = fin.compute_signed_bet_lengths(positions, "fill_with_zero")
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
        expected_bet_starts = pd.to_datetime(
            [
                "2010-01-04",
                "2010-01-07",
                "2010-01-09",
                "2010-01-10",
                "2010-01-11",
                "2010-01-12",
            ]
        )
        expected = pd.Series(
            [4, -3, 0, -1, 0, 1], index=expected_bet_starts, dtype=float
        )
        actual = fin.compute_signed_bet_lengths(positions, "fill_with_zero")
        pd.testing.assert_series_equal(actual, expected)

    def test4(self) -> None:
        """
        Test a single value.
        """
        positions = pd.Series([1], index=[pd.Timestamp("2010-01-01")])
        expected = pd.Series([1], index=[pd.Timestamp("2010-01-01")], dtype=float)
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test5(self) -> None:
        """
        Test NaNs.
        """
        idx = pd.to_datetime(["2010-01-01", "2010-01-02"])
        positions = pd.Series([np.nan, np.nan], index=idx)
        expected = pd.Series([0, 0], index=idx, dtype=float)
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test6(self) -> None:
        """
        Test NaNs.
        """
        idx = pd.to_datetime(["2010-01-01", "2010-01-02", "2010-01-03"])
        positions = pd.Series([np.nan, np.nan, 1], index=idx)
        expected = pd.Series([0, 0, 1], index=idx, dtype=float)
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test7(self) -> None:
        """
        Test NaNs
        """
        idx = pd.to_datetime(["2010-01-01", "2010-01-02", "2010-01-03"])
        positions = pd.Series([1, np.nan, np.nan], index=idx)
        expected = pd.Series([0, 0, 3], index=idx, dtype=float)
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test8(self) -> None:
        """
        Test zeroes.
        """
        idx = pd.to_datetime(["2010-01-01", "2010-01-02"])
        positions = pd.Series([0, 0], index=idx)
        expected = pd.Series()
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test9(self) -> None:
        """
        Test zeroes.
        """
        idx = pd.to_datetime(["2010-01-01", "2010-01-02", "2010-01-03"])
        positions = pd.Series([0, 0, 1], index=idx)
        expected = pd.Series([1.0], index=[pd.Timestamp("2010-01-03")])
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)

    def test10(self) -> None:
        """
        Test zeroes.
        """
        idx = pd.to_datetime(["2010-01-01", "2010-01-02", "2010-01-03"])
        positions = pd.Series([1, 0, 0], index=idx)
        expected = pd.Series(
            [1.0, 0.0],
            index=[pd.Timestamp("2010-01-01"), pd.Timestamp("2010-01-03")],
        )
        actual = fin.compute_signed_bet_lengths(positions)
        pd.testing.assert_series_equal(actual, expected)
