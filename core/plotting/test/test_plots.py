# %%
import logging
import unittest

# %%
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# %%
import core.plotting.correlation as cplocorr
import core.plotting.misc_plotting as cplmiplo
import core.plotting.visual_stationarity_test as cpvistte
import core.plotting.normality as cplonorm


# %%
_LOG = logging.getLogger(__name__)


# %%
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

    @staticmethod
    def get_plot_timeseries_distribution1() -> pd.Series:
        """
        Get test data for plotting time series distribution.
        """
        samples = [0] * 50
        index = pd.date_range(start="2022-12-31", periods=len(samples), freq="H")
        srs = pd.Series(samples, index=index, name="test values")
        return srs

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
    def get_plot_normal_distribution_data(size=100, seed=0) -> pd.Series:
        """
        Get a random normal distribution data series for the qq plot.

        :param size: size of generated data.
        :param seed: seed used to randomly generate data.
        :return: generated test data series.
        """
        rng = np.random.default_rng(seed=seed)
        samples = rng.normal(size=size)
        srs = pd.Series(samples)
        return srs

    @staticmethod
    def get_plot_exponential_distribution_data(size=100, seed=0) -> pd.Series:
        """
        Get a random exponential distribution data series for the qq plot.

        :param size: size of generated data.
        :param seed: seed used to randomly generate data.
        :return: generated test data series.
        """
        rng = np.random.default_rng(seed=seed)
        samples = rng.exponential(size=size)
        srs = pd.Series(samples)
        return srs

    @staticmethod
    def null_out_series_data(srs: pd.Series) -> pd.Series:
        """
        Randomly null out some data in the input series. 

        :param srs: series to be modified.
        :return: series with NaNs.
        """
        mask = np.random.choice([True, False], size=srs.shape, p=[0.2, 0.8])
        new_srs = srs.mask(mask)
        return new_srs

    def test_plot_histograms_and_lagged_scatterplot1(self) -> None:
        """
        Smoke test for `plot_histograms_and_lagged_scatterplot()`.
        """
        # Set inputs.
        srs = Test_plots.get_plot_histograms_and_lagged_scatterplot1()
        lag = 7
        # Plot.
        cpvistte.plot_histograms_and_lagged_scatterplot(
            srs, lag, figsize=(20, 10)
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
        cplocorr.plot_heatmap(corr_df, mode, figsize=(20, 10))

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

    def test_plot_timeseries_distribution1(self) -> None:
        """
        Smoke test for 'plot_timeseries_distribution()'.
        """
        srs = self.get_plot_timeseries_distribution1()
        datetime_types = ["hour"]
        cplmiplo.plot_timeseries_distribution(srs, datetime_types)

    def test_plot_timeseries_distribution2(self) -> None:
        """
        Smoke test for 'plot_timeseries_distribution()'.
        """
        srs = self.get_plot_timeseries_distribution1()
        datetime_types = ["hour", "month"]
        cplmiplo.plot_timeseries_distribution(srs, datetime_types)

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
        _, axes = plt.subplots(2, 2, figsize=(20, 10))
        axes_flat = axes.flatten()
        cplmiplo.plot_spectrum(signal=test_df, axes=axes_flat)

    def test_plot_qq1(self) -> None:
        """
        Smoke test for `plot_qq`.

        - `ax` is None
        """
        test_srs = self.get_plot_normal_distribution_data()
        cplonorm.plot_qq(test_srs)

    def test_plot_qq2(self) -> None:
        """
        Smoke test for `plot_qq`.

        - `ax` is Matplotlib axis.
        """
        test_srs = self.get_plot_normal_distribution_data()
        _, ax = plt.subplots(1, 1, figsize=(5, 5))
        cplonorm.plot_qq(test_srs, ax=ax)
    
    def test_plot_qq3(self) -> None:
        """
        Smoke test for `plot_qq`.

        - `dist` is exponential distribution.
        """
        test_srs = self.get_plot_exponential_distribution_data()
        _, ax = plt.subplot(1, 1, figsize=(5, 5))
        cplonorm.plot_qq(test_srs, ax=ax, dist='expon')
    
    def test_plot_qq4(self) -> None:
        """
        Smoke test for `plot_qq`.

        - NaNs present in the input series. 
        """
        test_srs = self.get_plot_normal_distribution_data()
        srs_nan = self.null_out_series_data(test_srs)
        _, ax = plt.subplot(1, 1, figsize=(5, 5))
        cplonorm.plot_qq(srs_nan, ax=ax)


# %%
