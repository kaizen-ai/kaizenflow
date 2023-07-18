import unittest

import pandas as pd
import numpy as np

import core.plotting.misc_plotting as cplmiplo


class Test_plot_timeseries_distribution(unittest.TestCase):
    @staticmethod
    def get_plot_timeseries_distribution(type: str) -> pd.Series:
        rng = np.random.default_rng(seed=0)
        samples = rng.normal(size=100)
        if type == "hour":
            frequency = "H"
        elif type == "month":
            frequency = "M"
        index = pd.date_range(start="2023-01-01", periods=len(samples), freq=frequency)
        srs = pd.Series(samples, index=index)
        return srs
    
    def test1(self) -> None:
        """
        Check hour period.
        """
        srs = self.get_plot_timeseries_distribution("hour")
        type = ["hour"]
        cplmiplo.plot_timeseries_distribution(srs, type)

    def test2(self) -> None:
        """
        Check month period.
        """
        srs = self.get_plot_timeseries_distribution("month")
        type = ["month"]
        cplmiplo.plot_timeseries_distribution(srs, type)
