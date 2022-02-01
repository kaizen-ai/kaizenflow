import logging
from typing import List

import numpy as np
import pandas as pd

import core.artificial_signal_generators as carsigen
import core.signal_processing as csigproc
import dataflow.model.forecast_evaluator as dtfmofoeva
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class TestForecastEvaluator1(hunitest.TestCase):
    def test_to_str_intraday_1_asset_floating_gmv(self) -> None:
        data = self.get_data(
            pd.Timestamp("2022-01-03 09:30:00", tz="America/New_York"),
            pd.Timestamp("2022-01-03 10:00:00", tz="America/New_York"),
            asset_ids=[101],
        )
        forecast_evaluator = dtfmofoeva.ForecastEvaluator(
            returns_col="rets",
            volatility_col="vol",
            prediction_col="pred",
        )
        actual = forecast_evaluator.to_str(data)
        expected = r"""
# holdings marked to market=
                            101
2022-01-03 09:30:00-05:00   NaN
2022-01-03 09:35:00-05:00   NaN
2022-01-03 09:40:00-05:00  1.02
2022-01-03 09:45:00-05:00 -0.92
2022-01-03 09:50:00-05:00 -1.04
2022-01-03 09:55:00-05:00  0.06
2022-01-03 10:00:00-05:00   NaN
# pnl=
                                101
2022-01-03 09:30:00-05:00       NaN
2022-01-03 09:35:00-05:00       NaN
2022-01-03 09:40:00-05:00       NaN
2022-01-03 09:45:00-05:00  2.73e-04
2022-01-03 09:50:00-05:00  2.30e-04
2022-01-03 09:55:00-05:00 -1.32e-04
2022-01-03 10:00:00-05:00  4.80e-05
# statistics=
                                pnl  gross_volume  net_volume   gmv   nmv
2022-01-03 09:30:00-05:00       NaN           NaN         NaN   NaN   NaN
2022-01-03 09:35:00-05:00       NaN           NaN         NaN   NaN   NaN
2022-01-03 09:40:00-05:00       NaN           NaN         NaN  1.02  1.02
2022-01-03 09:45:00-05:00  2.73e-04          1.95       -1.95  0.92 -0.92
2022-01-03 09:50:00-05:00  2.30e-04          0.12       -0.12  1.04 -1.04
2022-01-03 09:55:00-05:00 -1.32e-04          1.10        1.10  0.06  0.06
2022-01-03 10:00:00-05:00  4.80e-05           NaN         NaN   NaN   NaN"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_to_str_intraday_1_asset_targeted_gmv(self) -> None:
        data = self.get_data(
            pd.Timestamp("2022-01-03 09:30:00", tz="America/New_York"),
            pd.Timestamp("2022-01-03 10:00:00", tz="America/New_York"),
            asset_ids=[101],
        )
        forecast_evaluator = dtfmofoeva.ForecastEvaluator(
            returns_col="rets",
            volatility_col="vol",
            prediction_col="pred",
        )
        actual = forecast_evaluator.to_str(data, target_gmv=10000)
        expected = r"""
# holdings marked to market=
                               101
2022-01-03 09:30:00-05:00      NaN
2022-01-03 09:35:00-05:00      NaN
2022-01-03 09:40:00-05:00  10000.0
2022-01-03 09:45:00-05:00 -10000.0
2022-01-03 09:50:00-05:00 -10000.0
2022-01-03 09:55:00-05:00  10000.0
2022-01-03 10:00:00-05:00      NaN
# pnl=
                            101
2022-01-03 09:30:00-05:00   NaN
2022-01-03 09:35:00-05:00   NaN
2022-01-03 09:40:00-05:00   NaN
2022-01-03 09:45:00-05:00  2.67
2022-01-03 09:50:00-05:00  2.49
2022-01-03 09:55:00-05:00 -1.26
2022-01-03 10:00:00-05:00  8.43
# statistics=
                            pnl  gross_volume  net_volume      gmv      nmv
2022-01-03 09:30:00-05:00   NaN           NaN         NaN      NaN      NaN
2022-01-03 09:35:00-05:00   NaN           NaN         NaN      NaN      NaN
2022-01-03 09:40:00-05:00   NaN           NaN         NaN  10000.0  10000.0
2022-01-03 09:45:00-05:00  2.67       20000.0    -20000.0  10000.0 -10000.0
2022-01-03 09:50:00-05:00  2.49           0.0         0.0  10000.0 -10000.0
2022-01-03 09:55:00-05:00 -1.26       20000.0     20000.0  10000.0  10000.0
2022-01-03 10:00:00-05:00  8.43           NaN         NaN      NaN      NaN"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_to_str_intraday_3_assets_floating_gmv(self) -> None:
        data = self.get_data(
            pd.Timestamp("2022-01-03 09:30:00", tz="America/New_York"),
            pd.Timestamp("2022-01-03 10:00:00", tz="America/New_York"),
            asset_ids=[101, 201, 301],
        )
        forecast_evaluator = dtfmofoeva.ForecastEvaluator(
            returns_col="rets",
            volatility_col="vol",
            prediction_col="pred",
        )
        actual = forecast_evaluator.to_str(data)
        expected = r"""
# holdings marked to market=
                            101   201   301
2022-01-03 09:30:00-05:00   NaN   NaN   NaN
2022-01-03 09:35:00-05:00   NaN   NaN   NaN
2022-01-03 09:40:00-05:00  1.02 -2.30 -2.63
2022-01-03 09:45:00-05:00 -0.92  0.60  0.98
2022-01-03 09:50:00-05:00 -1.04  0.22  0.35
2022-01-03 09:55:00-05:00  0.06 -2.63 -0.18
2022-01-03 10:00:00-05:00   NaN   NaN   NaN
# pnl=
                                101       201       301
2022-01-03 09:30:00-05:00       NaN       NaN       NaN
2022-01-03 09:35:00-05:00       NaN       NaN       NaN
2022-01-03 09:40:00-05:00       NaN       NaN       NaN
2022-01-03 09:45:00-05:00  2.73e-04  4.26e-03  4.85e-03
2022-01-03 09:50:00-05:00  2.30e-04 -1.06e-04  1.67e-04
2022-01-03 09:55:00-05:00 -1.32e-04  9.40e-05 -6.10e-05
2022-01-03 10:00:00-05:00  4.80e-05  2.59e-03 -1.40e-05
# statistics=
                                pnl  gross_volume  net_volume   gmv   nmv
2022-01-03 09:30:00-05:00       NaN           NaN         NaN   NaN   NaN
2022-01-03 09:35:00-05:00       NaN           NaN         NaN   NaN   NaN
2022-01-03 09:40:00-05:00       NaN           NaN         NaN  5.96 -3.91
2022-01-03 09:45:00-05:00  9.39e-03          8.47        4.57  2.51  0.66
2022-01-03 09:50:00-05:00  2.90e-04          1.14       -1.14  1.61 -0.48
2022-01-03 09:55:00-05:00 -9.90e-05          4.48       -2.28  2.87 -2.75
2022-01-03 10:00:00-05:00  2.63e-03           NaN         NaN   NaN   NaN"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_to_str_intraday_3_assets_targeted_gmv(self) -> None:
        data = self.get_data(
            pd.Timestamp("2022-01-03 09:30:00", tz="America/New_York"),
            pd.Timestamp("2022-01-03 10:00:00", tz="America/New_York"),
            asset_ids=[101, 201, 301],
        )
        forecast_evaluator = dtfmofoeva.ForecastEvaluator(
            returns_col="rets",
            volatility_col="vol",
            prediction_col="pred",
        )
        actual = forecast_evaluator.to_str(data, target_gmv=100000)
        expected = r"""
# holdings marked to market=
                                101       201       301
2022-01-03 09:30:00-05:00       NaN       NaN       NaN
2022-01-03 09:35:00-05:00       NaN       NaN       NaN
2022-01-03 09:40:00-05:00  17185.97 -38629.90 -44184.13
2022-01-03 09:45:00-05:00 -36831.11  23956.26  39212.62
2022-01-03 09:50:00-05:00 -64790.20  13741.93  21467.87
2022-01-03 09:55:00-05:00   1988.18 -91783.22  -6228.60
2022-01-03 10:00:00-05:00       NaN       NaN       NaN
# pnl=
                            101    201    301
2022-01-03 09:30:00-05:00   NaN    NaN    NaN
2022-01-03 09:35:00-05:00   NaN    NaN    NaN
2022-01-03 09:40:00-05:00   NaN    NaN    NaN
2022-01-03 09:45:00-05:00  4.59  71.46  81.44
2022-01-03 09:50:00-05:00  9.15  -4.24   6.65
2022-01-03 09:55:00-05:00 -8.20   5.85  -3.78
2022-01-03 10:00:00-05:00  1.68  90.39  -0.48
# statistics=
                              pnl  gross_volume  net_volume       gmv       nmv
2022-01-03 09:30:00-05:00     NaN           NaN         NaN       NaN       NaN
2022-01-03 09:35:00-05:00     NaN           NaN         NaN       NaN       NaN
2022-01-03 09:40:00-05:00     NaN           NaN         NaN  100000.0 -65628.06
2022-01-03 09:45:00-05:00  157.49     200000.00    91965.84  100000.0  26337.78
2022-01-03 09:50:00-05:00   11.56      55918.18   -55918.18  100000.0 -29580.40
2022-01-03 09:55:00-05:00   -6.12     200000.00   -66443.24  100000.0 -96023.64
2022-01-03 10:00:00-05:00   91.59           NaN         NaN       NaN       NaN"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_to_str_multiday_1_asset_targeted_gmv(self) -> None:
        data = self.get_data(
            pd.Timestamp("2022-01-03 09:30:00", tz="America/New_York"),
            pd.Timestamp("2022-01-05 10:00:00", tz="America/New_York"),
            asset_ids=[101],
            bar_duration="1H",
        )
        forecast_evaluator = dtfmofoeva.ForecastEvaluator(
            returns_col="rets",
            volatility_col="vol",
            prediction_col="pred",
        )
        actual = forecast_evaluator.to_str(data, target_gmv=100000)
        expected = r"""
# holdings marked to market=
                                101
2022-01-03 09:30:00-05:00       NaN
2022-01-03 10:30:00-05:00       NaN
2022-01-03 11:30:00-05:00  100000.0
2022-01-03 12:30:00-05:00 -100000.0
2022-01-03 13:30:00-05:00 -100000.0
2022-01-03 14:30:00-05:00  100000.0
2022-01-03 15:30:00-05:00 -100000.0
2022-01-04 09:30:00-05:00 -100000.0
2022-01-04 10:30:00-05:00 -100000.0
2022-01-04 11:30:00-05:00 -100000.0
2022-01-04 12:30:00-05:00 -100000.0
2022-01-04 13:30:00-05:00 -100000.0
2022-01-04 14:30:00-05:00 -100000.0
2022-01-04 15:30:00-05:00  100000.0
2022-01-05 09:30:00-05:00       NaN
# pnl=
                             101
2022-01-03 09:30:00-05:00    NaN
2022-01-03 10:30:00-05:00    NaN
2022-01-03 11:30:00-05:00    NaN
2022-01-03 12:30:00-05:00  26.70
2022-01-03 13:30:00-05:00  24.85
2022-01-03 14:30:00-05:00 -12.65
2022-01-03 15:30:00-05:00  84.34
2022-01-04 09:30:00-05:00 -85.83
2022-01-04 10:30:00-05:00 -47.53
2022-01-04 11:30:00-05:00  45.07
2022-01-04 12:30:00-05:00  75.46
2022-01-04 13:30:00-05:00  81.45
2022-01-04 14:30:00-05:00  34.38
2022-01-04 15:30:00-05:00   5.14
2022-01-05 09:30:00-05:00 -97.18
# statistics=
                             pnl  gross_volume  net_volume       gmv       nmv
2022-01-03 09:30:00-05:00    NaN           NaN         NaN       NaN       NaN
2022-01-03 10:30:00-05:00    NaN           NaN         NaN       NaN       NaN
2022-01-03 11:30:00-05:00    NaN           NaN         NaN  100000.0  100000.0
2022-01-03 12:30:00-05:00  26.70      200000.0   -200000.0  100000.0 -100000.0
2022-01-03 13:30:00-05:00  24.85           0.0         0.0  100000.0 -100000.0
2022-01-03 14:30:00-05:00 -12.65      200000.0    200000.0  100000.0  100000.0
2022-01-03 15:30:00-05:00  84.34      200000.0   -200000.0  100000.0 -100000.0
2022-01-04 09:30:00-05:00 -85.83           0.0         0.0  100000.0 -100000.0
2022-01-04 10:30:00-05:00 -47.53           0.0         0.0  100000.0 -100000.0
2022-01-04 11:30:00-05:00  45.07           0.0         0.0  100000.0 -100000.0
2022-01-04 12:30:00-05:00  75.46           0.0         0.0  100000.0 -100000.0
2022-01-04 13:30:00-05:00  81.45           0.0         0.0  100000.0 -100000.0
2022-01-04 14:30:00-05:00  34.38           0.0         0.0  100000.0 -100000.0
2022-01-04 15:30:00-05:00   5.14      200000.0    200000.0  100000.0  100000.0
2022-01-05 09:30:00-05:00 -97.18           NaN         NaN       NaN       NaN"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_log_portfolio_read_portfolio(self) -> None:
        data = self.get_data(
            pd.Timestamp("2022-01-03 09:30:00", tz="America/New_York"),
            pd.Timestamp("2022-01-03 10:00:00", tz="America/New_York"),
            asset_ids=[101, 201, 301],
        )
        forecast_evaluator = dtfmofoeva.ForecastEvaluator(
            returns_col="rets",
            volatility_col="vol",
            prediction_col="pred",
        )
        #
        log_dir = self.get_scratch_space()
        file_name = forecast_evaluator.log_portfolio(
            data, log_dir, target_gmv=1e6
        )
        #
        portfolio_df, stats_df = forecast_evaluator.read_portfolio(
            log_dir, file_name
        )
        # Ensure that the `int` asset id type is recovered.
        asset_id_idx = portfolio_df.columns.levels[1]
        self.assertEqual(asset_id_idx.dtype.type, np.int64)
        #
        precision = 2
        portfolio_df_str = hpandas.df_to_str(
            portfolio_df, num_rows=None, precision=precision
        )
        expected_portfolio_df_str = r"""
                                      returns                     volatility                     prediction                       position                          pnl
                                101       201       301        101       201       301        101       201       301        101        201        301    101     201     301
2022-01-03 09:30:00-05:00       NaN       NaN       NaN        NaN       NaN       NaN        NaN       NaN       NaN        NaN        NaN        NaN    NaN     NaN     NaN
2022-01-03 09:35:00-05:00 -7.25e-04 -1.13e-03  6.30e-04   7.25e-04  1.13e-03  6.30e-04   7.42e-04 -2.61e-03 -1.66e-03        NaN        NaN        NaN    NaN     NaN     NaN
2022-01-03 09:40:00-05:00 -7.81e-04  3.06e-04 -2.38e-04   7.57e-04  7.84e-04  4.54e-04  -6.99e-04  4.71e-04  4.46e-04  171859.69 -386299.05 -441841.26    NaN     NaN     NaN
2022-01-03 09:45:00-05:00  2.67e-04 -1.85e-03 -1.84e-03   6.02e-04  1.34e-03  1.24e-03  -6.28e-04  2.96e-04  4.29e-04 -368311.12  239562.64  392126.24  45.89  714.64  814.42
2022-01-03 09:50:00-05:00 -2.49e-04 -1.77e-04  1.70e-04   5.07e-04  1.08e-03  1.01e-03   2.89e-05 -2.85e-03 -1.80e-04 -647902.00  137419.34  214678.66  91.54  -42.41   66.50
2022-01-03 09:55:00-05:00  1.26e-04  4.26e-04 -1.76e-04   4.27e-04  9.31e-04  8.42e-04        NaN       NaN       NaN   19881.78 -917832.21  -62286.01 -81.95   58.53  -37.78
2022-01-03 10:00:00-05:00  8.43e-04 -9.85e-04  7.68e-05   5.77e-04  9.47e-04  7.13e-04        NaN       NaN       NaN        NaN        NaN        NaN  16.77  903.95   -4.78"""
        self.assert_equal(
            portfolio_df_str, expected_portfolio_df_str, fuzzy_match=True
        )
        #
        stats_df_str = hpandas.df_to_str(
            stats_df, num_rows=None, precision=precision
        )
        expected_stats_df_str = r"""
                               pnl  gross_volume  net_volume        gmv        nmv
2022-01-03 09:30:00-05:00      NaN           NaN         NaN        NaN        NaN
2022-01-03 09:35:00-05:00      NaN           NaN         NaN        NaN        NaN
2022-01-03 09:40:00-05:00      NaN           NaN         NaN  1000000.0 -656280.62
2022-01-03 09:45:00-05:00  1574.95      2.00e+06   919658.37  1000000.0  263377.75
2022-01-03 09:50:00-05:00   115.63      5.59e+05  -559181.76  1000000.0 -295804.01
2022-01-03 09:55:00-05:00   -61.20      2.00e+06  -664432.42  1000000.0 -960236.43
2022-01-03 10:00:00-05:00   915.93           NaN         NaN        NaN        NaN"""
        self.assert_equal(stats_df_str, expected_stats_df_str, fuzzy_match=True)

    @staticmethod
    def get_data(
        start_datetime: pd.Timestamp,
        end_datetime: pd.Timestamp,
        asset_ids: List[int],
        *,
        bar_duration: str = "5T",
    ) -> pd.DataFrame:
        price_process = carsigen.PriceProcess(seed=10)
        dfs = {}
        for asset_id in asset_ids:
            price = price_process.generate_price_series_from_normal_log_returns(
                start_datetime,
                end_datetime,
                asset_id,
                bar_duration=bar_duration,
            )
            vol = csigproc.compute_rolling_norm(price.pct_change(), tau=4)
            rets = price.pct_change()
            noise_integral = (
                price_process.generate_price_series_from_normal_log_returns(
                    start_datetime,
                    end_datetime,
                    asset_id,
                    bar_duration=bar_duration,
                )
            )
            noise = noise_integral.pct_change()
            pred = rets.shift(-2) + noise
            df = pd.DataFrame(
                {
                    "rets": rets,
                    "vol": vol,
                    "pred": pred,
                }
            )
            dfs[asset_id] = df
        df = pd.concat(dfs.values(), axis=1, keys=dfs.keys())
        # Swap column levels so that symbols are leaves.
        df = df.swaplevel(i=0, j=1, axis=1)
        df.sort_index(axis=1, level=0, inplace=True)
        return df
