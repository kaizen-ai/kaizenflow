import logging

import numpy as np
import pandas as pd
import pytest

import core.pandas_helpers as pde
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)

# #############################################################################


class TestDfRollingApply(ut.TestCase):

    def test1(self):
        """
        Test with function returning a pd.Series.
        """
        np.random.seed(10)
        df = pd.DataFrame(np.random.rand(100, 2).round(2), columns=['A', 'B'])
        window = 5
        df_act = pde.df_rolling_apply(df, window, np.mean)
        #
        df_exp = df.rolling(window).apply(np.mean, raw=True)
        self.assert_equal(df_act.to_string(), df_exp.to_string())

    @pytest.mark.skip
    def test2(self):
        """
        Test with function returning a pd.DataFrame.
        """
        np.random.seed(10)
        func = lambda x: pd.DataFrame(np.mean(x))
        df = pd.DataFrame(np.random.rand(100, 2).round(2), columns=['A', 'B'])
        window = 5
        df_act = pde.df_rolling_apply(df, window, func)
        #
        df_exp = df.rolling(window).apply(func, raw=True)
        self.assert_equal(df_act.to_string(), df_exp.to_string())

    @pytest.mark.skip
    def test3(self):
        """
        Test with function returning a pd.DataFrame with multiple lines.
        """
        np.random.seed(10)
        func = lambda x: pd.DataFrame([np.mean(x), np.sum(x)])
        df = pd.DataFrame(np.random.rand(100, 2).round(2), columns=['A', 'B'])
        window = 5
        df_act = pde.df_rolling_apply(df, window, func)
        #
        df_exp = df.rolling(window).apply(func, raw=True)
        self.assert_equal(df_act.to_string(), df_exp.to_string())
