import logging

import numpy as np
import pandas as pd
import pytest

import core.explore as exp
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_explore1(hut.TestCase):
    def test_ols_regress_series(self) -> None:
        x = 5 * np.random.randn(100)
        y = x + np.random.randn(*x.shape)
        df = pd.DataFrame()
        df["x"] = x
        df["y"] = y
        exp.ols_regress_series(
            df["x"], df["y"], intercept=True, print_model_stats=False
        )

    @pytest.mark.skip(reason="https://github.com/.../.../issues/3676")
    def test_rolling_pca_over_time1(self) -> None:
        np.random.seed(42)
        df = pd.DataFrame(np.random.randn(10, 5))
        df.index = pd.date_range("2017-01-01", periods=10)
        corr_df, eigval_df, eigvec_df = exp.rolling_pca_over_time(
            df, 0.5, "fill_with_zero"
        )
        txt = (
            "corr_df=\n%s\n" % corr_df.to_string()
            + "eigval_df=\n%s\n" % eigval_df.to_string()
            + "eigvec_df=\n%s\n" % eigvec_df.to_string()
        )
        self.check_string(txt)
