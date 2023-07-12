import logging
import unittest

import numpy as np
import pandas as pd

import core.plotting.misc_plotting as cplmiplo
import core.plotting.visual_stationarity_test as cpvistte

_LOG = logging.getLogger(__name__)


class Test_plots(unittest.TestCase):
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

    def test_plot_time_series_by_period(self) -> None:
        """
        Smoke test for `plot_time_series_by_period()`.
        """
        # Create a test series with hourly timestamps
        timestamps = pd.date_range(start="2023-07-01", end="2023-07-02", freq="H")
        values = np.random.rand(len(timestamps))
        test_series = pd.Series(values, index=timestamps)

        # Test for "hour" period
        cplmiplo.plot_time_series_by_period(test_series, "hour")

        # Test for "time" period
        cplmiplo.plot_time_series_by_period(test_series, "time")
