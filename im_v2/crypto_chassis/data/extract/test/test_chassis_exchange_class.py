import pandas as pd

import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import im_v2.crypto_chassis.data.extract.exchange_class as imvccdeecl


class TestCryptoChassisExchange1(hunitest.TestCase):
    def test_initialize_class(self) -> None:
        """
        Smoke test that the class is being initialized correctly.
        """
        _ = imvccdeecl.CryptoChassisExchange()

    def test_download_market_depth_data1(
        self,
    ) -> None:
        """
        Test download for historical data.
        """
        start_timestamp = pd.Timestamp("2022-01-09T00:00:00", tz="UTC")
        exchange = "binance"
        currency_pair = "btc-usdt"
        client = imvccdeecl.CryptoChassisExchange()
        actual = client.download_market_depth(
            exchange, currency_pair, start_timestamp=start_timestamp
        )
        # Verify dataframe length.
        self.assertEqual(86007, actual.shape[0])
        # Verify corner datetime if output is not empty.
        first_date = int(actual["time_seconds"].iloc[0])
        last_date = int(actual["time_seconds"].iloc[-1])
        self.assertEqual(1641686400, first_date)
        self.assertEqual(1641772799, last_date)
        # Check the output values.
        actual = actual.reset_index(drop=True)
        actual = hpandas.convert_df_to_json_string(actual)
        self.check_string(actual)

    def test_download_market_depth_invalid_input1(self) -> None:
        """
        Run with invalid start timestamp.
        """
        exchange = "binance"
        currency_pair = "btc-usdt"
        start_timestamp = "invalid"
        expected = """
* Failed assertion *
Instance of 'invalid' is '<class 'str'>' instead of '<class 'pandas._libs.tslibs.timestamps.Timestamp'>'
"""
        client = imvccdeecl.CryptoChassisExchange()
        with self.assertRaises(AssertionError) as cm:
            client.download_market_depth(
                exchange, currency_pair, start_timestamp=start_timestamp
            )
        # Check output for error.
        actual = str(cm.exception)
        self.assertIn(expected, actual)

    def test_download_market_depth_invalid_input2(self) -> None:
        """
        Run with invalid exchange name.
        """
        exchange = "bibance"
        currency_pair = "btc-usdt"
        start_timestamp = pd.Timestamp("2022-01-09T00:00:00", tz="UTC")
        expected = """'urls'"""
        client = imvccdeecl.CryptoChassisExchange()
        with self.assertRaises(KeyError) as cm:
            client.download_market_depth(
                exchange, currency_pair, start_timestamp=start_timestamp
            )
        # Check output for error.
        actual = str(cm.exception)
        self.assert_equal(expected, actual, fuzzy_match=True)

    def test_download_market_depth_invalid_input3(self) -> None:
        """
        Run with invalid currency pair.
        """
        exchange = "binance"
        currency_pair = "btc-busdt"
        # End is before start -> invalid.
        start_timestamp = pd.Timestamp("2022-01-09T00:00:00", tz="UTC")
        expected = """'urls'"""
        client = imvccdeecl.CryptoChassisExchange()
        with self.assertRaises(KeyError) as cm:
            client.download_market_depth(
                exchange, currency_pair, start_timestamp=start_timestamp
            )
        # Check output for error.
        actual = str(cm.exception)
        self.assert_equal(expected, actual, fuzzy_match=True)
