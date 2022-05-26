import numpy as np
import pandas as pd

import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import research_amp.cc.qa as ramccqa


class TestGetBadDataStats(hunitest.TestCase):
    def test_get_bad_data_stats1(self) -> None:
        """
        Test that stats are computed correctly.
        """
        crypto_chassis_data = self._get_test_data()
        agg_level = ["full_symbol", "year", "month"]
        crypto_chassis_bad_data_stats = ramccqa.get_bad_data_stats(
            crypto_chassis_data, agg_level
        )
        crypto_chassis_bad_data_stats = hpandas.df_to_str(
            crypto_chassis_bad_data_stats
        )
        expected_signature = """
                                    bad data [%]  missing bars [%]  volume=0 [%]  NaNs [%]
        binance::ADA_USDT 2021 3          75.0              50.0          25.0       0.0
        ftx::BTC_USDT     2021 3         100.0               0.0           0.0     100.0
        """
        # Check.
        self.assert_equal(
            crypto_chassis_bad_data_stats, expected_signature, fuzzy_match=True
        )

    @staticmethod
    def _get_test_data() -> pd.DataFrame:
        index = [
            pd.Timestamp("2021-03-07 00:00:00+00:00"),
            pd.Timestamp("2021-03-07 00:03:00+00:00"),
            pd.Timestamp("2021-03-07 18:00:00+00:00"),
        ]
        data = {
            "full_symbol": [
                "binance::ADA_USDT",
                "binance::ADA_USDT",
                "ftx::BTC_USDT",
            ],
            "close": [1.409, 1.22, np.nan],
            "volume": [0, 12512.44, np.nan],
            "year": [2021, 2021, 2021],
            "month": [3, 3, 3],
        }
        df = pd.DataFrame(data, index=index)
        return df
