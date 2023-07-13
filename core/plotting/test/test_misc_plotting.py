import logging
import unittest

import pandas as pd
import core.plotting.misc_plotting as cplmiplo

class Test_plot_timeseries_distribution(unittest.TestCase):
    def test1(self)-> None:
        """
        Check hour period
        """
        d = {'a': 1, 'b': 2, 'c': 3}
        ser = pd.Series(data=d, index=['a', 'b', 'c'])
        cplmiplo.plot_timeseries_distribution(ser)
        
       # self.assertListEqual(actual, expected)
       
    def test2(self)-> None:
        """
        Check time period
        """
        
       # self.assertListEqual(actual, expected)