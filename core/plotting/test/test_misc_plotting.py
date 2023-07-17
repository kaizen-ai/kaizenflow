import unittest

import pandas as pd

import core.plotting.misc_plotting as cplmiplo


class Test_plot_timeseries_distribution(unittest.TestCase):
    def test1(self) -> None:
        """
        Check hour period.
        """
        idx = [
            pd.Timestamp("2000-01-01 9:01"),
            pd.Timestamp("2000-01-01 9:02"),
            pd.Timestamp("2000-01-01 9:03"),
            pd.Timestamp("2000-01-01 9:04"),
        ]
        value = [1, 2, 3, 4]
        srs = pd.Series(value, index=idx)
        type = ["hour"]
        cplmiplo.plot_timeseries_distribution(srs, type)

    def test2(self) -> None:
        """
        Check month period.
        """
        idx = [
            pd.Timestamp("2000-01-01 9:01"),
            pd.Timestamp("2000-01-01 9:02"),
            pd.Timestamp("2000-01-01 9:03"),
            pd.Timestamp("2000-01-01 9:04"),
        ]
        value = [1, 2, 3, 4]
        srs = pd.Series(value, index=idx)
        type = ["hour", "month"]
        cplmiplo.plot_timeseries_distribution(srs, type)
