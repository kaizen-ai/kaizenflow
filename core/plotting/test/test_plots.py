import logging
import unittest

import numpy as np
import pandas as pd

import core.plotting.correlation as cplocorr
import core.plotting.misc_plotting as cplmiplo
import core.plotting.visual_stationarity_test as cpvistte

_LOG = logging.getLogger(__name__)


class Test_plots(unittest.TestCase):
    """
    Run smoke tests for plotting functions.
    """

    @staticmethod
    def get_plot_histograms_and_lagged_scatterplot1() -> pd.Series:
        """
        Get a random Gaussian data series for plotting histograms and lagged
        scatterplot.
        """
        rng = np.random.default_rng(seed=0)
        samples = rng.normal(size=100)
        index = pd.date_range(start="2023-01-01", periods=len(samples), freq="D")
        srs = pd.Series(samples, index=index)
        return srs

    @staticmethod
    def get_plot_time_series_by_period1() -> pd.Series:
        """
        Generate a test time series with daily timestamps.
        """
        np.random.seed(35)
        timestamps = pd.date_range(
            start="2023-07-01", end="2023-07-07", freq="4H"
        )
        values = np.random.rand(len(timestamps))
        test_series = pd.Series(values, index=timestamps)
        return test_series

    @staticmethod
    def get_plot_heatmap() -> pd.core.frame.DataFrame:
        """
        Generate a data frame with some random features.
        """
        np.random.seed(35)
        df = pd.DataFrame(np.random.randn(10, 6), columns=list("ABCDEF"))
        return df

    def test_plot_histograms_and_lagged_scatterplot1(self) -> None:
        """
        Smoke test for `plot_histograms_and_lagged_scatterplot()`.
        """
        # Set inputs.
        srs = self.get_plot_histograms_and_lagged_scatterplot1()
        lag = 7
        # TODO(Dan): Remove after integration with `cmamp`. Changes from Cm #4722 are not in `sorrentum` yet.
        figsize = (20, 20)
        # Plot.
        cpvistte.plot_histograms_and_lagged_scatterplot(srs, lag, figsize=figsize)

    def test_plot_time_series_by_period1(self) -> None:
        test_series = self.get_plot_time_series_by_period1()
        period = "day"
        cplmiplo.plot_time_series_by_period(test_series, period)

    def test_plot_time_series_by_period2(self) -> None:
        test_series = self.get_plot_time_series_by_period1()
        period = "time"
        cplmiplo.plot_time_series_by_period(test_series, period)

    def test_plot_heatmap1(self) -> None:
        """
        Smoke test for `plot_heatmap()`
        """
        mode = "clustermap"
        corr_df = self.get_plot_heatmap()
        figsize = (20, 20)
        cplocorr.plot_heatmap(corr_df, mode, figsize=figsize)
