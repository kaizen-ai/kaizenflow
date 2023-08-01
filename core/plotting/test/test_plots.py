import logging
import unittest

import matplotlib.pyplot as plt
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
    def get_plot_spectrum1() -> pd.DataFrame:
        """
        Generate a test DataFrame for the plot_spectrum function.
        """
        data = {
            "signal1": np.random.randn(1000),
            "signal2": np.random.randn(1000),
        }
        df = pd.DataFrame(data)
        return df

    @staticmethod
    def get_plot_effective_correlation_rank1() -> pd.Series:
        """
        Generate a test DataFrame for plotting effective correlation rank.
        """
        data = {
            "Column1": np.random.rand(10),
            "Column2": np.random.rand(10),
            "Column3": np.random.rand(10),
            "Column4": np.random.rand(10),
        }
        test_df = pd.DataFrame(data)
        return test_df

    @staticmethod
    def get_plot_time_series_by_period1() -> pd.Series:
        """
        Generate a test time series with daily timestamps.
        """
        timestamps = pd.date_range(
            start="2023-07-01", end="2023-07-07", freq="4H"
        )
        values = np.random.rand(len(timestamps))
        test_series = pd.Series(values, index=timestamps)
        return test_series

    @staticmethod
    def get_plot_heatmap1() -> pd.core.frame.DataFrame:
        """
        Generate a data frame with some random features.
        """
        df = pd.DataFrame(np.random.randn(10, 6), columns=list("ABCDEF"))
        return df

    @classmethod
    def setUpClass(cls):
        # Save the original random seed.
        cls.original_seed = np.random.get_state()
        # Set a specific random seed for the entire test class.
        cls._seed = 42
        np.random.seed(42)
        # Set a specific figure size.
        cls._figsize = (20, 20)

    @classmethod
    def tearDownClass(cls):
        """
        Restore the original random seed after all the test methods have been
        executed.
        """
        np.random.set_state(cls.original_seed)

    def get_plot_histograms_and_lagged_scatterplot1(self) -> pd.Series:
        """
        Get a random Gaussian data series for plotting histograms and lagged
        scatterplot.
        """
        rng = np.random.default_rng(seed=self._seed)
        samples = rng.normal(size=100)
        index = pd.date_range(start="2023-01-01", periods=len(samples), freq="D")
        srs = pd.Series(samples, index=index)
        return srs

    def test_plot_histograms_and_lagged_scatterplot1(self) -> None:
        """
        Smoke test for `plot_histograms_and_lagged_scatterplot()`.
        """
        # Set inputs.
        srs = self.get_plot_histograms_and_lagged_scatterplot1()
        lag = 7
        # Plot.
        cpvistte.plot_histograms_and_lagged_scatterplot(
            srs, lag, figsize=self._figsize
        )

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
        corr_df = self.get_plot_heatmap1()
        cplocorr.plot_heatmap(corr_df, mode, figsize=self._figsize)

    def test_plot_effective_correlation_rank1(self) -> None:
        """
        Smoke test for `plot_effective_correlation_rank()`.

        - `q_values` is None
        """
        test_df = self.get_plot_effective_correlation_rank1()
        cplocorr.plot_effective_correlation_rank(test_df)

    def test_plot_effective_correlation_rank2(self) -> None:
        """
        Smoke test for `plot_effective_correlation_rank()`.

        - `q_values` is a list of values
        """
        num_q_values = 5
        q_values = np.random.uniform(1, 10, num_q_values).tolist()
        test_df = self.get_plot_effective_correlation_rank1()
        cplocorr.plot_effective_correlation_rank(test_df, q_values)

    def test_plot_spectrum1(self) -> None:
        """
        Smoke test for `plot_spectrum`.

        - `axes` is None
        """
        test_df = self.get_plot_spectrum1()
        cplmiplo.plot_spectrum(test_df)

    def test_plot_spectrum2(self) -> None:
        """
        Smoke test for `plot_spectrum`.

        - `axes` is a list of Matplotlib axes
        """
        test_df = self.get_plot_spectrum1()
        _, axes = plt.subplots(2, 2, figsize=self._figsize)
        axes_flat = axes.flatten()
        cplmiplo.plot_spectrum(signal=test_df, axes=axes_flat)
