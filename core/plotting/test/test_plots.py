import logging
import unittest

import numpy as np
import pandas as pd

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
        timestamps = pd.date_range(start="2023-07-01", end="2023-07-07", freq="4H")
        values = np.random.rand(len(timestamps))
        test_series = pd.Series(values, index=timestamps)
        return test_series

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

class Test_plot_timeseries_distribution(unittest.TestCase):
    @staticmethod
    def get_plot_timeseries_distribution1() -> pd.Series:
        """
        Get plot with hour datetype.
        """
        rng = np.random.default_rng(seed=0)
        samples = rng.normal(size=100)
        frequency = "H"
        index = pd.date_range(start="2023-01-01", periods=len(samples), freq=frequency)
        srs = pd.Series(samples, index=index)
        return srs
    
    def test_plot_timeseries_distribution1(self) -> None:
        srs = self.get_plot_timeseries_distribution1()
        datetime_types = ["hour"]
        cplmiplo.plot_timeseries_distribution(srs, datetime_types)
    
    @staticmethod
    def get_plot_timeseries_distribution3() -> pd.Series:
        """
        Get plot with month datetype.
        """
        rng = np.random.default_rng(seed=0)
        samples = rng.normal(size=100)
        frequency = "M"
        index = pd.date_range(start="2023-01-01", periods=len(samples), freq=frequency)
        srs = pd.Series(samples, index=index)
        return srs
    
    def test_plot_timeseries_distribution2(self) -> None:
        srs = self.get_plot_timeseries_distribution3()
        datetime_types = ["hour", "month"]
        cplmiplo.plot_timeseries_distribution(srs, datetime_types)