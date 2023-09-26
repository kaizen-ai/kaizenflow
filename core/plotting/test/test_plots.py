import logging
import unittest

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import core.plotting.boxplot as cploboxp
import core.plotting.correlation as cplocorr
import core.plotting.misc_plotting as cplmiplo
import core.plotting.normality as cplonorm
import core.plotting.visual_stationarity_test as cpvistte

_LOG = logging.getLogger(__name__)


class Test_plots(unittest.TestCase):
    """
    Run smoke tests for plotting functions.
    """

    @staticmethod
    def get_test_plot_df1() -> pd.DataFrame:
        """
        Generate a DataFrame with random values to test plotting functions.
        """
        data = {
            "Column1": np.random.rand(100),
            "Column2": np.random.rand(100),
            "Column3": np.random.rand(100),
            "Column4": np.random.rand(100),
        }
        df = pd.DataFrame(data)
        return df

    @staticmethod
    def get_test_plot_srs1() -> pd.Series:
        """
        Generate a test data with normal distribution series and timestamps.
        """
        rng = np.random.default_rng(seed=0)
        samples = rng.normal(size=100)
        index = pd.date_range(start="2023-01-31", periods=len(samples), freq="H")
        srs = pd.Series(samples, index=index, name="test values")
        return srs

    def test_plot_histograms_and_lagged_scatterplot1(self) -> None:
        """
        Smoke test for `plot_histograms_and_lagged_scatterplot()`.
        """
        # Set inputs.
        srs = Test_plots.get_test_plot_srs1()
        lag = 7
        # Plot.
        cpvistte.plot_histograms_and_lagged_scatterplot(
            srs, lag, figsize=(20, 10)
        )

    def test_plot_time_series_by_period1(self) -> None:
        test_series = self.get_test_plot_srs1()
        period = "day"
        cplmiplo.plot_time_series_by_period(test_series, period)

    def test_plot_time_series_by_period2(self) -> None:
        test_series = self.get_test_plot_srs1()
        period = "time"
        cplmiplo.plot_time_series_by_period(test_series, period)

    def test_plot_heatmap1(self) -> None:
        """
        Smoke test for `plot_heatmap()`
        """
        mode = "clustermap"
        corr_df = self.get_test_plot_df1()
        cplocorr.plot_heatmap(corr_df, mode, figsize=(20, 10))

    def test_plot_effective_correlation_rank1(self) -> None:
        """
        Smoke test for `plot_effective_correlation_rank()`.

        - `q_values` is None
        """
        test_df = self.get_test_plot_df1()
        cplocorr.plot_effective_correlation_rank(test_df)

    def test_plot_effective_correlation_rank2(self) -> None:
        """
        Smoke test for `plot_effective_correlation_rank()`.

        - `q_values` is a list of values
        """
        num_q_values = 5
        q_values = np.random.uniform(1, 10, num_q_values).tolist()
        test_df = self.get_test_plot_df1()
        cplocorr.plot_effective_correlation_rank(test_df, q_values)

    def test_plot_timeseries_distribution1(self) -> None:
        """
        Smoke test for 'plot_timeseries_distribution()'.
        """
        srs = self.get_test_plot_srs1()
        datetime_types = ["hour"]
        cplmiplo.plot_timeseries_distribution(srs, datetime_types)

    def test_plot_timeseries_distribution2(self) -> None:
        """
        Smoke test for 'plot_timeseries_distribution()'.
        """
        srs = self.get_test_plot_srs1()
        datetime_types = ["hour", "month"]
        cplmiplo.plot_timeseries_distribution(srs, datetime_types)

    def test_plot_spectrum1(self) -> None:
        """
        Smoke test for `plot_spectrum`.

        - `axes` is None
        """
        test_df = self.get_test_plot_df1()
        cplmiplo.plot_spectrum(test_df)

    def test_plot_spectrum2(self) -> None:
        """
        Smoke test for `plot_spectrum`.

        - `axes` is a list of Matplotlib axes
        """
        test_df = self.get_test_plot_df1()
        _, axes = plt.subplots(2, 2, figsize=(20, 10))
        axes_flat = axes.flatten()
        cplmiplo.plot_spectrum(signal=test_df, axes=axes_flat)

    def test_plot_cols1(self) -> None:
        """
        Smoke test for `plot_cols`.
        """
        test_df = self.get_test_plot_df1()
        cplmiplo.plot_cols(test_df)

    def test_plot_autocorrelation1(self) -> None:
        """
        Smoke test for `plot_autocorrelation()`.
        """
        test_df = self.get_test_plot_df1()
        cplmiplo.plot_autocorrelation(test_df)

    def test_plot_boxplot1(self) -> None:
        """
        Smoke test for `plot_boxplot`.

        - `grouping` is "by_row"
        - `ylabel` is an empty string
        """
        test_df = self.get_test_plot_df1()
        cploboxp.plot_boxplot(test_df)

    def test_plot_boxplot2(self) -> None:
        """
        Smoke test for `plot_boxplot`.

        - `grouping` is "by_col"
        - `ylabel` is a non-empty string
        """
        test_df = self.get_test_plot_df1()
        grouping = "by_col"
        ylabel = "Test Label"
        cploboxp.plot_boxplot(test_df, grouping=grouping, ylabel=ylabel)

    def test_plot_qq1(self) -> None:
        """
        Smoke test for `plot_qq()`.
        """
        test_series = self.get_test_plot_srs1()
        cplonorm.plot_qq(test_series)

    def test_plot_qq2(self) -> None:
        """
        Smoke test for `plot_qq()`.

        - input series contains NaN values
        - nan_mode = "drop"
        - axes are passed
        """
        test_series = self.get_test_plot_srs1()
        test_series[20:50] = np.nan
        _, axes = plt.subplots(1, 1, figsize=(10, 10))
        dist = "norm"
        nan_mode = "drop"
        cplonorm.plot_qq(test_series, ax=axes, dist=dist, nan_mode=nan_mode)
