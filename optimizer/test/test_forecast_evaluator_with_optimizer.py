import logging

import pandas as pd

import helpers.hunit_test as hunitest
import optimizer.forecast_evaluator_with_optimizer as ofevwiop

_LOG = logging.getLogger(__name__)


class TestForecastEvaluatorWithOptimizer1(hunitest.TestCase):
    @staticmethod
    def get_data() -> pd.DataFrame:
        tz = "America/New_York"
        idx = [
            pd.Timestamp("2022-01-03 09:35:00", tz=tz),
            pd.Timestamp("2022-01-03 09:40:00", tz=tz),
            pd.Timestamp("2022-01-03 09:45:00", tz=tz),
            pd.Timestamp("2022-01-03 09:50:00", tz=tz),
        ]
        asset_ids = [100, 200]
        prediction_data = [
            [-0.25, -0.34],
            [0.13, 0.5],
            [0.84, -0.97],
            [0.86, -0.113],
        ]
        price_data = [
            [100.0, 100.3],
            [100.1, 100.5],
            [100.05, 100.4],
            [100.2, 100.5],
        ]
        volatility_data = [
            [0.00110, 0.00048],
            [0.00091, 0.00046],
            [0.00086, 0.00060],
            [0.00071, 0.00068],
        ]
        prediction_df = pd.DataFrame(prediction_data, idx, asset_ids)
        price_df = pd.DataFrame(price_data, idx, asset_ids)
        volatility_df = pd.DataFrame(volatility_data, idx, asset_ids)
        dag_df = pd.concat(
            {
                "price": price_df,
                "volatility": volatility_df,
                "prediction": prediction_df,
            },
            axis=1,
        )
        return dag_df

    @staticmethod
    def get_config_dict() -> dict:
        dict_ = {
            "dollar_neutrality_penalty": 0.0,
            "volatility_penalty": 0.0,
            "relative_holding_penalty": 0.0,
            "relative_holding_max_frac_of_gmv": 0.6,
            "target_gmv": 3000,
            "target_gmv_upper_bound_penalty": 0.0,
            "target_gmv_hard_upper_bound_multiple": 1.00,
            "turnover_penalty": 0.0,
            "solver": "ECOS",
        }
        return dict_

    def test_to_str1(self) -> None:
        data = self.get_data()
        config_dict = self.get_config_dict()
        forecast_evaluator = ofevwiop.ForecastEvaluatorWithOptimizer(
            price_col="price",
            volatility_col="volatility",
            prediction_col="prediction",
            optimizer_config_dict=config_dict,
        )
        actual = forecast_evaluator.to_str(
            data,
            quantization="nearest_share",
        )
        expected = """
        # holdings_shares=
                                    100   200
        2022-01-03 09:35:00-05:00   0.0   0.0
        2022-01-03 09:40:00-05:00 -12.0 -18.0
        2022-01-03 09:45:00-05:00  12.0  18.0
        2022-01-03 09:50:00-05:00   0.0   0.0
        # holdings_notional=
                                      100     200
        2022-01-03 09:35:00-05:00     0.0     0.0
        2022-01-03 09:40:00-05:00 -1201.2 -1809.0
        2022-01-03 09:45:00-05:00  1200.6  1807.2
        2022-01-03 09:50:00-05:00     0.0     0.0
        # executed_trades_shares=
                                    100   200
        2022-01-03 09:35:00-05:00   0.0   0.0
        2022-01-03 09:40:00-05:00 -12.0 -18.0
        2022-01-03 09:45:00-05:00  24.0  36.0
        2022-01-03 09:50:00-05:00 -12.0 -18.0
        # executed_trades_notional=
                                      100     200
        2022-01-03 09:35:00-05:00     0.0     0.0
        2022-01-03 09:40:00-05:00 -1201.2 -1809.0
        2022-01-03 09:45:00-05:00  2401.2  3614.4
        2022-01-03 09:50:00-05:00 -1202.4 -1809.0
        # pnl=
                                   100  200
        2022-01-03 09:35:00-05:00  0.0  0.0
        2022-01-03 09:40:00-05:00  0.0  0.0
        2022-01-03 09:45:00-05:00  0.6  1.8
        2022-01-03 09:50:00-05:00  1.8  1.8
        # statistics=
                                   pnl  gross_volume  net_volume     gmv     nmv
        2022-01-03 09:35:00-05:00  0.0           0.0         0.0     0.0     0.0
        2022-01-03 09:40:00-05:00  0.0        3010.2     -3010.2  3010.2 -3010.2
        2022-01-03 09:45:00-05:00  2.4        6015.6      6015.6  3007.8  3007.8
        2022-01-03 09:50:00-05:00  3.6        3011.4     -3011.4     0.0     0.0
        """
        self.assert_equal(actual, expected, fuzzy_match=True)
