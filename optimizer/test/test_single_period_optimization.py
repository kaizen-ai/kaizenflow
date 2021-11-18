import io
import logging

import numpy as np
import pandas as pd
import pytest

import helpers.unit_test as huntes

_LOG = logging.getLogger(__name__)

# TODO(gp): Use `hprintin.color_highlight`.
_WARNING = "\033[33mWARNING\033[0m"

try:
    import cvxpy as cvx
    _HAS_CVXPY = True
except ImportError as e:
    print(_WARNING + ": " + str(e))
    _HAS_CVXPY = False

if _HAS_CVXPY:
    import optimizer.costs as optcos
    import optimizer.single_period_optimization as optspo


@pytest.mark.skip(reason="Requires special docker container.")
class Test_compute_single_period_optimization(huntes.TestCase):
    def test1(self) -> None:
        """
        Test with basic covariance matrix cost.
        """
        holdings_txt = """
asset,dollars
T1,0
T2,0
T3,0
cash,1
"""
        holdings = pd.read_csv(io.StringIO(holdings_txt), index_col=0).squeeze()
        predictions_txt = """
asset,ret
T1,-0.1
T2,-0.1
T3,0.1
cash,0
"""
        predictions = pd.read_csv(
            io.StringIO(predictions_txt), index_col=0
        ).squeeze()
        #
        covariance = self._get_covariance()
        gamma = 0.001
        costs = [optcos.CovarianceRiskModel(covariance, gamma)]
        #
        spo = optspo.SinglePeriodOptimizer(costs, [])
        actual = spo.optimize(holdings, predictions)
        #
        txt = """
asset,target_positions,target_trades,target_weights,target_weight_diffs
T1,-336.03,-336.03,-336.03,-336.03
T2,-336.03,-336.03,-336.03,-336.03
T3,663.97,663.97,663.97,663.97
cash,9.10,8.10,9.10,8.10
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0)
        self.assert_dfs_close(actual, expected, rtol=1e-2, atol=1e-2)

    def test2(self) -> None:
        """
        Test with basic covariance matrix cost rescaled.
        """
        holdings_txt = """
asset,dollars
T1,0
T2,0
T3,0
cash,100
"""
        holdings = pd.read_csv(io.StringIO(holdings_txt), index_col=0).squeeze()
        predictions_txt = """
asset,ret
T1,-0.1
T2,-0.1
T3,0.1
cash,0
"""
        predictions = pd.read_csv(
            io.StringIO(predictions_txt), index_col=0
        ).squeeze()
        #
        covariance = self._get_covariance()
        gamma = 1.0
        costs = [optcos.CovarianceRiskModel(covariance, gamma)]
        #
        spo = optspo.SinglePeriodOptimizer(costs, [])
        actual = spo.optimize(holdings, predictions)
        #
        txt = """
asset,target_positions,target_trades,target_weights,target_weight_diffs
T1,-16.38,-16.38,-0.16,-0.16
T2,-16.38,-16.38,-0.16,-0.16
T3,83.62,83.62,0.84,0.84
cash,49.14,-50.86,0.49,-0.51
"""
        expected = pd.read_csv(io.StringIO(txt), index_col=0)
        self.assert_dfs_close(actual, expected, rtol=1e-2, atol=1e-2)

    @staticmethod
    def _get_covariance() -> pd.DataFrame:
        mat = np.array(
            [
                [1, 0.9, 0.9, 0],
                [0.9, 1, 0.9, 0],
                [0.9, 0.9, 1, 0],
                [0, 0, 0, 1],
            ]
        )
        names = ["T1", "T2", "T3", "cash"]
        covariance = pd.DataFrame(mat, index=names, columns=names)
        return covariance
