import logging

import numpy as np
import pandas as pd

import core.pandas_helpers as pde
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)

# #############################################################################


class TestDfRollingApply(ut.TestCase):

    def test1(self):
        np.random.seed(10)
        df = pd.DataFrame(np.random.rand(100, 2).round(2), columns=['A', 'B'])
        window = 5
        df_act = pde.df_rolling_apply(df, window, np.mean)
        #
        df_exp = df.rolling(window).apply(np.mean, raw=True)
        self.assert_equal(df_act.to_string(), df_exp.to_string())
