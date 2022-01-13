import pandas as pd

import helpers.hprint as hprint
import helpers.hunit_test as hunitest
import market_data.market_data_client_example as mdmdclex

# TODO(gp): -> test_market_data_im_client.py

# TODO(gp): -> TestMarketDataImClient
class TestMarketDataClient(hunitest.TestCase):
    def test_get_data_for_interval1(self) -> None:
        """
        Test that data is loaded correctly.

        Checked conditions:
        - interval type is [a, b)
        - column names are remapped
        """
        # Build MarketDataInterface.
        asset_ids = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        columns = []
        column_remap = {"full_symbol": "asset_id"}
        market_data_client = mdmdclex.get_MarketDataInterface_example1(
            asset_ids, columns, column_remap
        )
        # Read data.
        start_ts = pd.Timestamp("2018-08-17T00:01:00")
        end_ts = pd.Timestamp("2018-08-17T00:05:00")
        ts_col_name = "end_ts"
        data = market_data_client.get_data_for_interval(
            start_ts,
            end_ts,
            ts_col_name,
            asset_ids,
            left_close=True,
            right_close=False,
            normalize_data=True,
            limit=None,
        )
        actual_df_as_str = hprint.df_to_short_str("df", data)
        # pylint: disable=line-too-long
        expected_df_as_str = """
        # df=
        df.index in [2018-08-16 20:01:00-04:00, 2018-08-16 20:04:00-04:00]
        df.columns=asset_id,open,high,low,close,volume,currency_pair,exchange_id,start_ts
        df.shape=(8, 9)
                                            asset_id         open         high          low        close     volume currency_pair exchange_id                  start_ts
        end_ts
        2018-08-16 20:01:00-04:00  binance::BTC_USDT  6311.640000  6311.770000  6302.810000  6302.810000  16.781206      BTC_USDT     binance 2018-08-16 20:00:00-04:00
        2018-08-16 20:01:00-04:00   kucoin::ETH_USDT   286.712987   286.712987   286.712987   286.712987   0.017500      ETH_USDT      kucoin 2018-08-16 20:00:00-04:00
        2018-08-16 20:02:00-04:00  binance::BTC_USDT  6302.810000  6306.000000  6292.790000  6297.260000  55.373226      BTC_USDT     binance 2018-08-16 20:01:00-04:00
        ...
        2018-08-16 20:03:00-04:00   kucoin::ETH_USDT   285.400193   285.400193   285.400193   285.400193   0.020260      ETH_USDT      kucoin 2018-08-16 20:02:00-04:00
        2018-08-16 20:04:00-04:00  binance::BTC_USDT  6294.520000  6299.980000  6290.000000  6296.100000  22.088586      BTC_USDT     binance 2018-08-16 20:03:00-04:00
        2018-08-16 20:04:00-04:00   kucoin::ETH_USDT   285.400193   285.884638   285.400193   285.884638   0.074655      ETH_USDT      kucoin 2018-08-16 20:03:00-04:00"""
        # pylint: enable=line-too-long
        self.assert_equal(
            actual_df_as_str,
            expected_df_as_str,
            dedent=True,
            fuzzy_match=True,
        )

    def test_get_data_for_interval2(self) -> None:
        """
        Test that data is loaded correctly.

        Checked conditions:
        - interval type is (a, b]
        - columns are filtered
        """
        # Build MarketDataInterface.
        asset_ids = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        columns = [
            "full_symbol",
            "close",
            "volume",
            "currency_pair",
            "exchange_id",
        ]
        column_remap = None
        market_data_client = mdmdclex.get_MarketDataInterface_example1(
            asset_ids, columns, column_remap
        )
        # Read data.
        start_ts = pd.Timestamp("2018-08-17T00:01:00")
        end_ts = pd.Timestamp("2018-08-17T00:05:00")
        ts_col_name = "end_ts"
        data = market_data_client.get_data_for_interval(
            start_ts,
            end_ts,
            ts_col_name,
            asset_ids,
            left_close=False,
            right_close=True,
            normalize_data=True,
            limit=None,
        )
        actual_df_as_str = hprint.df_to_short_str("df", data)
        # pylint: disable=line-too-long
        expected_df_as_str = """
        # df=
        df.index in [2018-08-16 20:02:00-04:00, 2018-08-16 20:05:00-04:00]
        df.columns=full_symbol,close,volume,currency_pair,exchange_id,start_ts
        df.shape=(8, 6)
                                         full_symbol        close     volume  currency_pair exchange_id                  start_ts
        end_ts
        2018-08-16 20:02:00-04:00  binance::BTC_USDT  6297.260000  55.373226      BTC_USDT     binance 2018-08-16 20:01:00-04:00
        2018-08-16 20:02:00-04:00   kucoin::ETH_USDT   285.400197   0.162255      ETH_USDT      kucoin 2018-08-16 20:01:00-04:00
        2018-08-16 20:03:00-04:00  binance::BTC_USDT  6294.520000  34.611797      BTC_USDT     binance 2018-08-16 20:02:00-04:00
        ...
        2018-08-16 20:04:00-04:00   kucoin::ETH_USDT   285.884638   0.074655      ETH_USDT      kucoin 2018-08-16 20:03:00-04:00
        2018-08-16 20:05:00-04:00  binance::BTC_USDT  6294.990000  18.986206      BTC_USDT     binance 2018-08-16 20:04:00-04:00
        2018-08-16 20:05:00-04:00   kucoin::ETH_USDT   285.884637   0.006141      ETH_USDT      kucoin 2018-08-16 20:04:00-04:00
        """
        # pylint: enable=line-too-long
        self.assert_equal(
            actual_df_as_str,
            expected_df_as_str,
            dedent=True,
            fuzzy_match=True,
        )

    def test_get_data_for_interval3(self) -> None:
        """
        Test that not normalized data is loaded correctly.
        """
        # Build `MarketDataInterface`.
        asset_ids = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        columns = []
        column_remap = {"full_symbol": "asset_id"}
        market_data_client = mdmdclex.get_MarketDataInterface_example1(
            asset_ids, columns, column_remap
        )
        # Read data.
        start_ts = pd.Timestamp("2018-08-17T00:01:00")
        end_ts = pd.Timestamp("2018-08-17T00:05:00")
        ts_col_name = "end_ts"
        data = market_data_client.get_data_for_interval(
            start_ts,
            end_ts,
            ts_col_name,
            asset_ids,
            left_close=True,
            right_close=False,
            normalize_data=False,
            limit=None,
        )
        actual_df_as_str = hprint.df_to_short_str("df", data)
        # pylint: disable=line-too-long
        expected_df_as_str = """
        # df=
        df.index in [2018-08-17 00:01:00+00:00, 2018-08-17 00:04:00+00:00]
        df.columns=asset_id,open,high,low,close,volume,currency_pair,exchange_id
        df.shape=(8, 8)
                                            asset_id         open         high          low        close     volume  currency_pair exchange_id
        timestamp
        2018-08-17 00:01:00+00:00  binance::BTC_USDT  6311.640000  6311.770000  6302.810000  6302.810000  16.781206       BTC_USDT     binance
        2018-08-17 00:01:00+00:00   kucoin::ETH_USDT   286.712987   286.712987   286.712987   286.712987   0.017500       ETH_USDT      kucoin
        2018-08-17 00:02:00+00:00  binance::BTC_USDT  6302.810000  6306.000000  6292.790000  6297.260000  55.373226       BTC_USDT     binance
        ...
        2018-08-17 00:03:00+00:00   kucoin::ETH_USDT   285.400193   285.400193   285.400193   285.400193   0.020260       ETH_USDT      kucoin
        2018-08-17 00:04:00+00:00  binance::BTC_USDT  6294.520000  6299.980000  6290.000000  6296.100000  22.088586       BTC_USDT     binance
        2018-08-17 00:04:00+00:00   kucoin::ETH_USDT   285.400193   285.884638   285.400193   285.884638   0.074655       ETH_USDT      kucoin"""
        # pylint: enable=line-too-long
        self.assert_equal(
            actual_df_as_str,
            expected_df_as_str,
            dedent=True,
            fuzzy_match=True,
        )

    def test_get_twap_price1(self) -> None:
        """
        Test that TWAP is computed correctly.
        """
        # Build MarketDataInterface.
        asset_ids = ["binance::BTC_USDT"]
        columns = []
        column_remap = None
        market_data_client = mdmdclex.get_MarketDataInterface_example1(
            asset_ids, columns, column_remap
        )
        # Compute TWAP price.
        start_ts = pd.Timestamp("2018-08-17T00:01:00")
        end_ts = pd.Timestamp("2018-08-17T00:05:00")
        ts_col_name = "end_ts"
        asset_id = asset_ids[0]
        actual = market_data_client.get_twap_price(
            start_ts, end_ts, ts_col_name, asset_id, column="close"
        ).round(2)
        self.assertEqual(actual, 6295.72)

    def test_should_be_online1(self) -> None:
        """
        Test that the interface is available at the given time.
        """
        # Build MarketDataInterface.
        asset_ids = ["binance::BTC_USDT"]
        columns = []
        column_remap = None
        market_data_client = mdmdclex.get_MarketDataInterface_example1(
            asset_ids, columns, column_remap
        )
        # Check.
        wall_clock_time = pd.Timestamp("2018-08-17T00:01:00")
        actual = market_data_client.should_be_online(wall_clock_time)
        self.assertEqual(actual, True)

    def test_get_last_end_time1(self) -> None:
        """
        Test that a call for the last end time is causing an error for now.
        """
        # Build MarketDataInterface.
        asset_ids = ["binance::BTC_USDT"]
        columns = []
        column_remap = None
        market_data_client = mdmdclex.get_MarketDataInterface_example1(
            asset_ids, columns, column_remap
        )
        # Check.
        actual = market_data_client._get_last_end_time()
        self.assertEqual(actual, NotImplementedError)
