import logging

import numpy as np
import pandas as pd

import core.artificial_signal_generators as carsigen
import dataflow.model.stats_computer as dtfmostcom
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class TestStatsComputer1(hunitest.TestCase):
    def test_compute_portfolio_stats1(self) -> None:
        sc = dtfmostcom.StatsComputer()
        portfolio_df = self._get_portfolio()
        df = sc.compute_portfolio_stats(portfolio_df, "1T")
        actual = hpandas.dataframe_to_str(df, precision=2)
        expected = r"""
ratios      sharpe_ratio                         5.78
            sharpe_ratio_standard_error          7.09
            sr.tval                              0.82
            sr.pval                              0.41
            kratio                              -0.67
dollar      gmv_mean                       1000000.00
            gmv_stdev                            0.00
            annualized_mean_return          146146.43
            annualized_volatility            25290.66
            max_drawdown                      4442.06
            turnover_mean                    99889.71
            turnover_stdev                    9849.70
            market_bias_mean                     0.00
            market_bias_stdev                    0.00
percentage  annualized_mean_return               0.15
            annualized_volatility                0.03
            max_drawdown                         0.00
            turnover_mean                        0.10
            turnover_stdev                       0.01
            market_bias_mean                     0.00
            market_bias_stdev                    0.00
dtype: float64"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def _get_portfolio(self, seed=1347, asset_id=100) -> pd.DataFrame:
        start_timestamp = pd.Timestamp(
            "2001-01-03 09:30:00-05:00", tz="America/New_York"
        )
        end_timestamp = pd.Timestamp(
            "2001-01-10 16:00:00-05:00", tz="America/New_York"
        )
        price_process = carsigen.PriceProcess(seed)
        price = price_process.generate_price_series_from_normal_log_returns(
            start_timestamp,
            end_timestamp,
            asset_id,
        )
        volume = price_process.generate_volume_series_from_poisson_process(
            start_timestamp,
            end_timestamp,
            asset_id,
        )
        gmv = 1e6
        pnl = 1e6 * 0.25 * np.log(price).diff()
        df = pd.DataFrame(
            {
                "pnl": pnl / 4,
                "gross_volume": 1e3 * volume,
                "net_volume": 0,
                "gmv": gmv,
                "nmv": 0,
            }
        )
        return df
