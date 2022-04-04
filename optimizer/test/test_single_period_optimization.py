import logging
from typing import Optional

import pandas as pd
import pytest

import core.config as cconfig
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import optimizer.single_period_optimization as osipeopt

_LOG = logging.getLogger(__name__)

# All tests in this file belong to the `optimizer` test list
# and need to be run inside an `opt` container.
pytestmark = pytest.mark.optimizer


class Test_SinglePeriodOptimizer1(hunitest.TestCase):
    @staticmethod
    def only_gmv_constraint_helper(solver: Optional[str] = None) -> str:
        dict_ = {
            "volatility_penalty": 0.0,
            "dollar_neutrality_penalty": 0.0,
            "turnover_penalty": 0.0,
            "target_gmv": 3000,
            "target_gmv_upper_bound_multiple": 1.00,
        }
        if solver is not None:
            dict_["solver"] = solver
        config = cconfig.get_config_from_nested_dict(dict_)
        df = Test_SinglePeriodOptimizer1.get_prediction_df()
        actual = Test_SinglePeriodOptimizer1.helper(config, df, restrictions=None)
        return actual

    @staticmethod
    def get_prediction_df() -> pd.DataFrame:
        df = pd.DataFrame(
            [[1, 1000, 0.05, 0.05], [2, 1500, 0.09, 0.07], [3, -500, 0.03, 0.08]],
            range(0, 3),
            ["asset_id", "position", "prediction", "volatility"],
        )
        return df

    @staticmethod
    def helper(
        config: cconfig.Config,
        df: pd.DataFrame,
        restrictions: Optional[pd.DataFrame],
    ) -> str:
        spo = osipeopt.SinglePeriodOptimizer(
            config, df, restrictions=restrictions
        )
        optimized = spo.optimize()
        #  stats = spo.compute_stats(optimized)
        precision = 2
        actual_str = hpandas.df_to_str(
            optimized.round(precision), precision=precision
        )
        return actual_str

    def test_only_gmv_constraint(self) -> None:
        actual = self.only_gmv_constraint_helper()
        expected = r"""
          target_position  target_notional_trade  target_weight  target_weight_diff
asset_id
1                   -0.00               -1000.00           -0.0                -1.0
2                 2999.94                1499.94            3.0                 1.5
3                   -0.00                 500.00           -0.0                 0.5"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_only_gmv_constraint_osqp(self) -> None:
        actual = self.only_gmv_constraint_helper("OSQP")
        expected = r"""
          target_position  target_notional_trade  target_weight  target_weight_diff
asset_id
1                   -0.00               -1000.00           -0.0                -1.0
2                 2999.94                1499.94            3.0                 1.5
3                   -0.00                 500.00           -0.0                 0.5"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_only_gmv_constraint_ecos(self) -> None:
        actual = self.only_gmv_constraint_helper("ECOS")
        expected = r"""
          target_position  target_notional_trade  target_weight  target_weight_diff
asset_id
1                     0.0                -1000.0            0.0                -1.0
2                  3000.0                 1500.0            3.0                 1.5
3                     0.0                  500.0            0.0                 0.5"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_only_gmv_constraint_scs(self) -> None:
        actual = self.only_gmv_constraint_helper("SCS")
        expected = r"""
          target_position  target_notional_trade  target_weight  target_weight_diff
asset_id
1                    -0.0                -1000.0           -0.0                -1.0
2                  3000.0                 1500.0            3.0                 1.5
3                     0.0                  500.0            0.0                 0.5"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_restrictions(self) -> None:
        dict_ = {
            "volatility_penalty": 0.0,
            "dollar_neutrality_penalty": 0.0,
            "turnover_penalty": 0.0,
            "target_gmv": 3000,
            "target_gmv_upper_bound_multiple": 1.00,
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        df = Test_SinglePeriodOptimizer1.get_prediction_df()
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
        actual = Test_SinglePeriodOptimizer1.helper(
            config, df, restrictions=restrictions
        )
        expected = r"""
          target_position  target_notional_trade  target_weight  target_weight_diff
asset_id
1                 1500.05                 500.05            1.5                 0.5
2                 1499.99                  -0.01            1.5                -0.0
3                    0.00                 500.00            0.0                 0.5"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_mixed_constraints(self) -> None:
        dict_ = {
            "volatility_penalty": 0.75,
            "dollar_neutrality_penalty": 0.1,
            "turnover_penalty": 0.0,
            "target_gmv": 3000,
            "target_gmv_upper_bound_multiple": 1.01,
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        df = Test_SinglePeriodOptimizer1.get_prediction_df()
        actual = Test_SinglePeriodOptimizer1.helper(config, df, restrictions=None)
        expected = r"""
          target_position  target_notional_trade  target_weight  target_weight_diff
asset_id
1                   -0.00               -1000.00          -0.00               -1.00
2                 1514.98                  14.98           1.51                0.01
3                -1514.97               -1014.97          -1.51               -1.01"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_short_ban(self) -> None:
        dict_ = {
            "volatility_penalty": 0.75,
            "dollar_neutrality_penalty": 0.1,
            "turnover_penalty": 0.0,
            "target_gmv": 3000,
            "target_gmv_upper_bound_multiple": 1.01,
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        df = Test_SinglePeriodOptimizer1.get_prediction_df()
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
        actual = Test_SinglePeriodOptimizer1.helper(
            config, df, restrictions=restrictions
        )
        expected = r"""
          target_position  target_notional_trade  target_weight  target_weight_diff
asset_id
1                -1015.07               -2015.07          -1.02               -2.02
2                 1515.03                  15.03           1.52                0.02
3                 -499.96                   0.04          -0.50                0.00"""
        self.assert_equal(actual, expected, fuzzy_match=True)


class Test_SinglePeriodOptimizer2(hunitest.TestCase):
    @staticmethod
    def get_prediction_df() -> pd.DataFrame:
        df = pd.DataFrame(
            [
                [101, 7734.32, 0.000858, 0.000910],
                [201, -10962.44, 0.000426, 0.000231],
                [301, -39037.56, -0.001845, 0.001404],
                [401, 42265.68, 0.000505, 0.000240],
            ],
            range(0, 4),
            ["asset_id", "position", "prediction", "volatility"],
        )
        return df

    @staticmethod
    def helper(
        config: cconfig.Config,
        df: pd.DataFrame,
    ) -> str:
        spo = osipeopt.SinglePeriodOptimizer(config, df)
        optimized = spo.optimize()
        precision = 2
        actual_str = hpandas.df_to_str(
            optimized.round(precision), precision=precision
        )
        return actual_str

    def test1(self) -> None:
        dict_ = {
            "volatility_penalty": 0.75,
            "dollar_neutrality_penalty": 0.1,
            "turnover_penalty": 0.0005,
            "target_gmv": 1e5,
            "target_gmv_upper_bound_multiple": 1.01,
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        df = Test_SinglePeriodOptimizer2.get_prediction_df()
        actual = Test_SinglePeriodOptimizer2.helper(config, df)
        expected = r"""
          target_position  target_notional_trade  target_weight  target_weight_diff
asset_id
101               8234.32                 500.00           0.33                0.02
201                  0.00               10962.44           0.00                0.44
301             -50500.00              -11462.44          -2.02               -0.46
401              42265.68                  -0.00           1.69               -0.00"""
        self.assert_equal(actual, expected, fuzzy_match=True)
