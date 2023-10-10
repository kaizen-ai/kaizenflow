import unittest.mock as umock

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import im_v2.binance.data.extract.extractor as ivbdexex


class TestBinanceExtractor(hunitest.TestCase):
    def _get_mock_trades(self) -> pd.DataFrame:
        """
        Returns a mock trades dataframe.
        """
        return pd.DataFrame(
            {
                "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT", "BTCUSDT"],
                "time": [
                    hdateti.convert_timestamp_to_unix_epoch(
                        pd.Timestamp("2020-01-01 00:00:00")
                    ),
                    hdateti.convert_timestamp_to_unix_epoch(
                        pd.Timestamp("2020-01-02 00:00:00")
                    ),
                    hdateti.convert_timestamp_to_unix_epoch(
                        pd.Timestamp("2020-01-03 00:00:00")
                    ),
                    hdateti.convert_timestamp_to_unix_epoch(
                        pd.Timestamp("2020-01-04 10:00:00")
                    ),
                ],
                "price": [100, 200, 300, 400],
                "qty": [1, 2, 3, 4],
                "is_buyer_maker": [True, False, True, False],
                "quote_qty": [100, 400, 900, 300],
                "id": [1, 2, 3, 4],
            }
        )

    def test_fetch_trades(self) -> None:
        """
        Tests the fetch trades method.
        """
        # Mock downloading files and extracting data from them.
        ivbdexex.hio = umock.MagicMock()
        contract_type = "futures"
        binance_extractor = ivbdexex.BinanceExtractor(
            contract_type, ivbdexex.BinanceNativeTimePeriod.DAILY
        )
        binance_extractor._download_binance_files = umock.MagicMock()
        binance_extractor._extract_data_from_binance_files = umock.MagicMock(
            return_value=self._get_mock_trades()
        )
        # Prepare parameters.
        currency_pair = "BTCUSDT"
        start_date = pd.Timestamp("2020-01-01 00:00:00")
        end_date = pd.Timestamp("2020-01-03 23:59:00")
        # Run.
        actual_df = binance_extractor._fetch_trades(
            currency_pair, start_date, end_date
        )
        # Compare results.
        actual = hpandas.df_to_str(
            actual_df.drop(columns=["end_download_timestamp"])
        )
        expected = """timestamp  price  amount  side
            0  1577836800000    100       1   buy
            1  1577923200000    200       2  sell
            2  1578009600000    300       3   buy"""
        self.assert_equal(actual, expected, fuzzy_match=True)
