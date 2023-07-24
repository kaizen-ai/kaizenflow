import logging
import unittest

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import core.plotting.visual_stationarity_test as cpvistte
import core.plotting.misc_plotting as cpmiscplt

_LOG = logging.getLogger(__name__)


class Test_plots(unittest.TestCase):
    def test_plot_histograms_and_lagged_scatterplot1(self) -> None:
        """
        Smoke test for `plot_histograms_and_lagged_scatterplot()`.
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

    def test_plot_projection(self) -> None:
        """
        Smoke test for `plot_projection()`.
        """
        rand_df = pd.DataFrame(np.random.randint(0,100,size=(4500, 7)), columns=['y1','y2','y3','y4','y5','y6','y7'])        
        fig = plt.figure()
        ax = fig.add_axes([0,0,1,1])
        cpmiscplt.plot_projection(rand_df, special_values = [1000], mode = "scatter", ax = ax)


