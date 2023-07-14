import logging
import unittest
import numpy as np
import pandas as pd
import core.plotting.misc_plotting as cplmiplo

class Test_plot_timeseries_distribution(unittest.TestCase):
    def test1(self)-> None:
        """
        Check hour period
        """
        idx = [
            pd.Timestamp("2000-01-01 9:01"),
            pd.Timestamp("2000-01-01 9:02"),
            pd.Timestamp("2000-01-01 9:03"),
            pd.Timestamp("2000-01-01 9:04"),
        ]
        value = [1.1,1.2,1.3,1.4]
        srs = pd.Series(value, index = idx)
        type = ["hour"]
        cplmiplo.plot_timeseries_distribution(srs,type)
        
       #pytest core/plotting/test/test_misc_plotting.py::Test_plot_timeseries_distribution::test1
    def test2(self)-> None:
        """
        Check time period
        """
        idx = [
            pd.Timestamp("2000-01-01 9:01"),
            pd.Timestamp("2000-01-01 9:02"),
            pd.Timestamp("2000-01-01 9:03"),
            pd.Timestamp("2000-01-01 9:04"),
        ]
        value = [0,0,0,0]
        srs = pd.Series(value, index = idx)
        type = []
        cplmiplo.plot_timeseries_distribution(srs,type)