import logging
from typing import Optional

import pandas as pd
import pytest

import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import optimizer.single_period_optimization as osipeopt

_LOG = logging.getLogger(__name__)


def _run_optimizer(
    config_dict: dict,
    df: pd.DataFrame,
    *,
    restrictions: Optional[pd.DataFrame],
) -> str:
    """
    Run the optimizer on the given df with the passed restrictions.
    """
    spo = osipeopt.SinglePeriodOptimizer(
        config_dict, df, restrictions=restrictions
    )
    optimized = spo.optimize(quantization=0)
    # Round to the nearest cent to reduce jitter.
    precision = 2
    actual_str = hpandas.df_to_str(
        optimized.round(precision), precision=precision
    )
    return actual_str


# #############################################################################
# TestSinglePeriodOptimizer1
# #############################################################################


class TestSinglePeriodOptimizer1(hunitest.TestCase):
    @staticmethod
    def get_prediction_df() -> pd.DataFrame:
        df = pd.DataFrame(
            [
                [1, 1000, 1, 1000, 0.05, 0.05],
                [2, 1500, 1, 1500, 0.09, 0.07],
                [3, -500, 1, -500, 0.03, 0.08],
            ],
            range(0, 3),
            [
                "asset_id",
                "holdings_shares",
                "price",
                "holdings_notional",
                "prediction",
                "volatility",
            ],
        )
        return df

    def run_opt_with_only_gmv_constraint(
        self, solver: Optional[str] = None
    ) -> str:
        dict_ = {
            "dollar_neutrality_penalty": 0.0,
            "volatility_penalty": 0.0,
            "relative_holding_penalty": 0.0,
            "relative_holding_max_frac_of_gmv": 1.0,
            "target_gmv": 3000,
            "target_gmv_upper_bound_penalty": 0.0,
            "target_gmv_hard_upper_bound_multiple": 1.00,
            "turnover_penalty": 0.0,
        }
        if solver is not None:
            dict_["solver"] = solver
        df = self.get_prediction_df()
        actual = _run_optimizer(dict_, df, restrictions=None)
        return actual

    # ///////////////////////////////////////////////////////////////////////////////

    def test_only_gmv_constraint(self) -> None:
        actual = self.run_opt_with_only_gmv_constraint()
        expected = r"""
          holdings_shares  price  holdings_notional  prediction  volatility  target_holdings_shares  target_holdings_notional  target_trades_shares  target_trades_notional
asset_id
1                    1000      1               1000        0.05        0.05                     0.0                       0.0               -1000.0                 -1000.0
2                    1500      1               1500        0.09        0.07                  3000.0                    3000.0                1500.0                  1500.0
3                    -500      1               -500        0.03        0.08                     0.0                       0.0                 500.0                   500.0"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_only_gmv_constraint_osqp(self) -> None:
        actual = self.run_opt_with_only_gmv_constraint("OSQP")
        expected = r"""
          holdings_shares  price  holdings_notional  prediction  volatility  target_holdings_shares  target_holdings_notional  target_trades_shares  target_trades_notional
asset_id
1                    1000      1               1000        0.05        0.05                     0.0                       0.0               -1000.0                 -1000.0
2                    1500      1               1500        0.09        0.07                  3000.0                    3000.0                1500.0                  1500.0
3                    -500      1               -500        0.03        0.08                     0.0                       0.0                 500.0                   500.0"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_only_gmv_constraint_ecos(self) -> None:
        actual = self.run_opt_with_only_gmv_constraint("ECOS")
        expected = r"""
          holdings_shares  price  holdings_notional  prediction  volatility  target_holdings_shares  target_holdings_notional  target_trades_shares  target_trades_notional
asset_id
1                    1000      1               1000        0.05        0.05                     0.0                       0.0               -1000.0                 -1000.0
2                    1500      1               1500        0.09        0.07                  3000.0                    3000.0                1500.0                  1500.0
3                    -500      1               -500        0.03        0.08                     0.0                       0.0                 500.0                   500.0
"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_only_gmv_constraint_scs(self) -> None:
        actual = self.run_opt_with_only_gmv_constraint("SCS")
        expected = r"""
          holdings_shares  price  holdings_notional  prediction  volatility  target_holdings_shares  target_holdings_notional  target_trades_shares  target_trades_notional
asset_id
1                    1000      1               1000        0.05        0.05                     0.0                       0.0               -1000.0                 -1000.0
2                    1500      1               1500        0.09        0.07                  3000.0                    3000.0                1500.0                  1500.0
3                    -500      1               -500        0.03        0.08                     0.0                       0.0                 500.0                   500.0
"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    # ///////////////////////////////////////////////////////////////////////////////

    @pytest.mark.skip(reason="See CmTask5114.")
    def test_restrictions(self) -> None:
        dict_ = {
            "dollar_neutrality_penalty": 0.0,
            "volatility_penalty": 0.0,
            "relative_holding_penalty": 0.0,
            "relative_holding_max_frac_of_gmv": 1.0,
            "target_gmv": 3000,
            "target_gmv_upper_bound_penalty": 0.0,
            "target_gmv_hard_upper_bound_multiple": 1.00,
            "turnover_penalty": 0.0,
        }
        df = self.get_prediction_df()
        restrictions = pd.DataFrame(
            [[2, True, True, True, True]],
            range(0, 1),
            [
                "asset_id",
                "is_buy_restricted",
                "is_buy_cover_restricted",
                "is_sell_short_restricted",
                "is_sell_long_restricted",
            ],
        )
        actual = _run_optimizer(dict_, df, restrictions=restrictions)
        expected = r"""
          holdings_shares  price  holdings_notional  prediction  volatility  target_holdings_shares  target_holdings_notional  target_trades_shares  target_trades_notional
asset_id
1                    1000      1               1000        0.05        0.05                  1500.0                    1500.0                 500.0                   500.0
2                    1500      1               1500        0.09        0.07                  1500.0                    1500.0                   0.0                     0.0
3                    -500      1               -500        0.03        0.08                     0.0                       0.0                 500.0                   500.0
"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_mixed_constraints(self) -> None:
        dict_ = {
            "dollar_neutrality_penalty": 0.1,
            "volatility_penalty": 0.75,
            "relative_holding_penalty": 0.0,
            "relative_holding_max_frac_of_gmv": 1.0,
            "target_gmv": 3000,
            "target_gmv_upper_bound_penalty": 0.0,
            "target_gmv_hard_upper_bound_multiple": 1.01,
            "turnover_penalty": 0.0,
        }
        df = self.get_prediction_df()
        actual = _run_optimizer(dict_, df, restrictions=None)
        expected = r"""
          holdings_shares  price  holdings_notional  prediction  volatility  target_holdings_shares  target_holdings_notional  target_trades_shares  target_trades_notional
asset_id
1                    1000      1               1000        0.05        0.05                     0.0                       0.0               -1000.0                 -1000.0
2                    1500      1               1500        0.09        0.07                  1515.0                    1515.0                  15.0                    15.0
3                    -500      1               -500        0.03        0.08                 -1515.0                   -1515.0               -1015.0                 -1015.0
"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_short_ban(self) -> None:
        dict_ = {
            "dollar_neutrality_penalty": 0.1,
            "volatility_penalty": 0.75,
            "relative_holding_penalty": 0.0,
            "relative_holding_max_frac_of_gmv": 1.0,
            "target_gmv": 3000,
            "target_gmv_upper_bound_penalty": 0.0,
            "target_gmv_hard_upper_bound_multiple": 1.01,
            "turnover_penalty": 0.0,
        }
        df = self.get_prediction_df()
        restrictions = pd.DataFrame(
            [[3, False, False, True, False]],
            range(0, 1),
            [
                "asset_id",
                "is_buy_restricted",
                "is_buy_cover_restricted",
                "is_sell_short_restricted",
                "is_sell_long_restricted",
            ],
        )
        actual = _run_optimizer(dict_, df, restrictions=restrictions)
        expected = r"""
          holdings_shares  price  holdings_notional  prediction  volatility  target_holdings_shares  target_holdings_notional  target_trades_shares  target_trades_notional
asset_id
1                    1000      1               1000        0.05        0.05                 -1015.0                   -1015.0               -2015.0                 -2015.0
2                    1500      1               1500        0.09        0.07                  1515.0                    1515.0                  15.0                    15.0
3                    -500      1               -500        0.03        0.08                  -500.0                    -500.0                   0.0                     0.0
"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_correlation_risk_model(self) -> None:
        dict_ = {
            "dollar_neutrality_penalty": 0.1,
            "volatility_penalty": 0.75,
            "relative_holding_penalty": 0.0,
            "relative_holding_max_frac_of_gmv": 1.0,
            "target_gmv": 3000,
            "target_gmv_upper_bound_penalty": 0.0,
            "target_gmv_hard_upper_bound_multiple": 1.01,
            "turnover_penalty": 0.0,
            "constant_correlation": 0.8,
            "constant_correlation_penalty": 5.0,
        }
        df = self.get_prediction_df()
        actual = _run_optimizer(dict_, df, restrictions=None)
        expected = r"""
          holdings_shares  price  holdings_notional  prediction  volatility  target_holdings_shares  target_holdings_notional  target_trades_shares  target_trades_notional
asset_id
1                    1000      1               1000        0.05        0.05                  -458.0                    -458.0               -1458.0                 -1458.0
2                    1500      1               1500        0.09        0.07                  1515.0                    1515.0                  15.0                    15.0
3                    -500      1               -500        0.03        0.08                 -1057.0                   -1057.0                -557.0                  -557.0
"""
        self.assert_equal(actual, expected, fuzzy_match=True)


# #############################################################################
# TestSinglePeriodOptimizer2
# #############################################################################


class TestSinglePeriodOptimizer2(hunitest.TestCase):
    @staticmethod
    def get_prediction_df() -> pd.DataFrame:
        df = pd.DataFrame(
            [
                [101, 7734.32, 0.000858, 0.000910, 0.05, 0.05],
                [201, -10962.44, 0.000426, 0.000231, 0.09, 0.07],
                [301, -39037.56, -0.001845, 0.001404, 0.03, 0.08],
                [401, 42265.68, 0.000505, 0.000240, 0.08, 0.03],
            ],
            range(0, 4),
            [
                "asset_id",
                "holdings_shares",
                "price",
                "holdings_notional",
                "prediction",
                "volatility",
            ],
        )
        return df

    def test1(self) -> None:
        dict_ = {
            "dollar_neutrality_penalty": 0.1,
            "volatility_penalty": 0.75,
            "relative_holding_penalty": 0.0,
            "relative_holding_max_frac_of_gmv": 1.0,
            "target_gmv": 1e5,
            "target_gmv_upper_bound_penalty": 0.0,
            "target_gmv_hard_upper_bound_multiple": 1.01,
            "turnover_penalty": 0.0005,
        }
        df = self.get_prediction_df()
        restrictions = None
        actual = _run_optimizer(dict_, df, restrictions=restrictions)
        expected = r"""
        holdings_shares  price  holdings_notional  prediction  volatility  target_holdings_shares  target_holdings_notional  target_trades_shares  target_trades_notional
asset_id
101               7734.32    0.0                0.0        0.05        0.05               -1.37e+02                     -0.12             -7.87e+03                   -6.75
201             -10962.44    0.0                0.0        0.09        0.07                8.58e+07                  36560.16              8.58e+07                36564.83
301             -39037.56    0.0                0.0        0.03        0.08                2.74e+07                 -50499.60              2.74e+07               -50571.62
401              42265.68    0.0                0.0        0.08        0.03                2.76e+07                  13939.71              2.76e+07                13918.37"""
        self.assert_equal(actual, expected, fuzzy_match=True)
