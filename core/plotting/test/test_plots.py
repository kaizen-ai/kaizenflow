import logging
import unittest

import numpy as np
import pandas as pd

import core.plotting.misc_plotting as cplmiplo
import core.plotting.visual_stationarity_test as cpvistte

_LOG = logging.getLogger(__name__)


class Test_plots(unittest.TestCase):
    @staticmethod
    def get_plot_time_series_by_period1() -> pd.Series:
        """
        Generate a test time series with hourly timestamps.
        """
        # Create a test series with hourly timestamps.
        timestamps = pd.date_range(
            start="2023-07-01", end="2023-07-02", freq="4H"
        )
        np.random.seed(35)
        values = np.random.rand(len(timestamps))
        test_series = pd.Series(values, index=timestamps)
        return test_series

    def test_plot_histograms_and_lagged_scatterplot1(self) -> None:
        """
        Smoke test for `plot_histograms_and_lagged_scatterplot()`.
        """
        # Set inputs.
        seq = np.concatenate(
            [np.random.uniform(-1, 1, 100), np.random.choice([5, 10], 100)]
        )
        index = pd.date_range(start="2023-01-01", periods=len(seq), freq="D")
        srs = pd.Series(seq, index=index)
        lag = 7
        # TODO(Dan): Remove after integration with `cmamp`
        figsize = (20, 20)
        # Plot.
        cpvistte.plot_histograms_and_lagged_scatterplot(srs, lag, figsize=figsize)

    def test_plot_time_series_by_period1(self) -> None:
        """
        Smoke test for `plot_time_series_by_period()` for periods: "hour".
        """
        # Set inputs.
        test_series = self.get_plot_time_series_by_period1()
        # Run smoke tests.
        period = "hour"
        cplmiplo.plot_time_series_by_period(test_series, period)

    def test_plot_time_series_by_period2(self) -> None:
        """
        Smoke test for `plot_time_series_by_period()` for periods: "time".
        """
        # Set inputs.
        test_series = self.get_plot_time_series_by_period1()
        # Run smoke tests.
        period = "time"
        cplmiplo.plot_time_series_by_period(test_series, period) 
