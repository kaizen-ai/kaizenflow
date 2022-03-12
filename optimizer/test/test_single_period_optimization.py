import logging
from typing import Optional

import pandas as pd

import core.config as cconfig
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import optimizer.single_period_optimization as osipeopt

_LOG = logging.getLogger(__name__)


class Test_SinglePeriodOptimizer1(hunitest.TestCase):
    def test_only_gmv_constraint(self) -> None:
        dict_ = {
            "volatility_penalty": 0.0,
            "dollar_neutrality_penalty": 0.0,
            "turnover_penalty": 0.0,
            "target_gmv": 3000,
            "target_gmv_upper_bound_multiple": 1.00,
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        df = Test_SinglePeriodOptimizer1.get_prediction_df()
        actual = Test_SinglePeriodOptimizer1.helper(config, df, restrictions=None)
        expected = r"""
          target_position  target_notional_trade  target_weight  target_weight_diff
asset_id
1                    8.44                -991.56           0.00               -0.33
2                 3089.38                1589.38           1.03                0.53
3                    8.44                 508.44           0.00                0.17"""
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
1                 1566.10                 566.10           0.52                0.19
2                 1487.98                 -12.02           0.50               -0.00
3                    2.75                 502.75           0.00                0.17"""
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
1                    1.10                -998.90            0.0               -0.33
2                 1496.51                  -3.49            0.5               -0.00
3                -1488.48                -988.48           -0.5               -0.33"""
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
1                -1018.65               -2018.65          -0.34               -0.67
2                 1515.80                  15.80           0.51                0.01
3                 -497.69                   2.31          -0.17                0.00"""
        self.assert_equal(actual, expected, fuzzy_match=True)

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


class Test_SinglePeriodOptimizer2(hunitest.TestCase):
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
101               7618.49                -115.83           0.08               -0.00
201                -43.19               10919.25          -0.00                0.11
301             -50135.64              -11098.08          -0.50               -0.11
401              42599.66                 333.98           0.43                0.00"""
        self.assert_equal(actual, expected, fuzzy_match=True)

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
