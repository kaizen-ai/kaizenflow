import logging
import unittest

import numpy as np
import pandas as pd

import core.plotting.visual_stationarity_test as cpvistte

_LOG = logging.getLogger(__name__)


class Test_plots(unittest.TestCase):
    def test_plot_histograms_and_lagged_scatterplots1(self) -> None:
        """
        Smoke test for `plot_histograms_and_lagged_scatterplots()`.
        """
        # Set inputs.
        seq = np.concatenate([np.random.uniform(-1, 1, 100), np.random.choice([5, 10], 100)])
        index = pd.date_range(start="2023-01-01", periods=len(seq), freq="D")
        srs = pd.Series(seq, index=index)
        lag = 7
        # TODO(Dan): Remove after integration with `cmamp`
        figsize = (20,20)
        # Plot.
        cpvistte.plot_histograms_and_lagged_scatterplot(srs, lag, figsize=figsize)


