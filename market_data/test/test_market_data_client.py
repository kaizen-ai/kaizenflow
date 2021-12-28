import os

import pandas as pd

import helpers.datetime_ as hdateti
import helpers.git as hgit
import helpers.printing as hprint
import helpers.unit_test as hunitest
import im_v2.ccxt.data.client.clients as imvcdclcl
import im_v2.common.data.client as imvcdcli
import market_data.market_data_client as mdmadacl


class TestMarketDataClient(hunitest.TestCase):
    def test_get_data_for_interval1(self) -> None:
        """
        Test that data is loaded correctly when:
            - interval type is [a; b)
            - column names are remapped
        """
        # Initialize the `MarketDataInterface`.
        multiple_symbols_client = self._helper()
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        market_data_client = mdmadacl.MarketDataInterface(
            "full_symbol",
            full_symbols,
            "start_ts",
            "end_ts",
            [],
            hdateti.get_current_time,
            im_client=multiple_symbols_client,
            column_remap={"full_symbol": "asset_id"},
        )
        # Read data.
        start_ts = pd.Timestamp("2018-08-17T00:01:00")
        end_ts = pd.Timestamp("2018-08-17T00:05:00")
        data = market_data_client.get_data_for_interval(
            start_ts,
            end_ts,
            "end_ts",
            full_symbols,
            left_close=True,
            right_close=False,
            normalize_data=True,
            limit=None,
        )
        actual_df_as_str = hprint.df_to_short_str("df", data)
        # pylint: disable=line-too-long
        expected_df_as_str = """
        # df=
        df.index in [2018-08-17 00:01:00+00:00, 2018-08-17 00:04:00+00:00]
        df.columns=asset_id,open,high,low,close,volume,epoch,currency_pair,exchange_id,start_ts
        df.shape=(8, 10)
                                            asset_id         open         high          low        close     volume          epoch currency_pair exchange_id                  start_ts
        end_ts
        2018-08-17 00:01:00+00:00  binance::BTC_USDT  6311.640000  6311.770000  6302.810000  6302.810000  16.781206  1534464060000      BTC_USDT     binance 2018-08-17 00:00:00+00:00
        2018-08-17 00:01:00+00:00   kucoin::ETH_USDT   286.712987   286.712987   286.712987   286.712987   0.017500  1534464060000      ETH_USDT      kucoin 2018-08-17 00:00:00+00:00
        2018-08-17 00:02:00+00:00  binance::BTC_USDT  6302.810000  6306.000000  6292.790000  6297.260000  55.373226  1534464120000      BTC_USDT     binance 2018-08-17 00:01:00+00:00
        ...
        2018-08-17 00:03:00+00:00   kucoin::ETH_USDT   285.400193   285.400193   285.400193   285.400193   0.020260  1534464180000      ETH_USDT      kucoin 2018-08-17 00:02:00+00:00
        2018-08-17 00:04:00+00:00  binance::BTC_USDT  6294.520000  6299.980000  6290.000000  6296.100000  22.088586  1534464240000      BTC_USDT     binance 2018-08-17 00:03:00+00:00
        2018-08-17 00:04:00+00:00   kucoin::ETH_USDT   285.400193   285.884638   285.400193   285.884638   0.074655  1534464240000      ETH_USDT      kucoin 2018-08-17 00:03:00+00:00
        """
        # pylint: enable=line-too-long
        self.assert_equal(
            actual_df_as_str,
            expected_df_as_str,
            dedent=True,
            fuzzy_match=True,
        )

    def test_get_data_for_interval2(self) -> None:
        """
        Test that data is loaded correctly when:
            - interval type is (a; b]
            - columns are filtered
        """
        # Initialize the `MarketDataInterface`.
        multiple_symbols_client = self._helper()
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        market_data_client = mdmadacl.MarketDataInterface(
            "full_symbol",
            full_symbols,
            "start_ts",
            "end_ts",
            ["full_symbol", "close", "volume", "currency_pair", "exchange_id"],
            hdateti.get_current_time,
            im_client=multiple_symbols_client,
        )
        # Read data.
        start_ts = pd.Timestamp("2018-08-17T00:01:00")
        end_ts = pd.Timestamp("2018-08-17T00:05:00")
        data = market_data_client.get_data_for_interval(
            start_ts,
            end_ts,
            "end_ts",
            full_symbols,
            left_close=False,
            right_close=True,
            normalize_data=True,
            limit=None,
        )
        actual_df_as_str = hprint.df_to_short_str("df", data)
        # pylint: disable=line-too-long
        expected_df_as_str = """
        # df=
        df.index in [2018-08-17 00:02:00+00:00, 2018-08-17 00:05:00+00:00]
        df.columns=full_symbol,close,volume,currency_pair,exchange_id,start_ts
        df.shape=(8, 6)
                                         full_symbol        close     volume  currency_pair exchange_id                  start_ts
        end_ts
        2018-08-17 00:02:00+00:00  binance::BTC_USDT  6297.260000  55.373226       BTC_USDT     binance 2018-08-17 00:01:00+00:00
        2018-08-17 00:02:00+00:00   kucoin::ETH_USDT   285.400197   0.162255       ETH_USDT      kucoin 2018-08-17 00:01:00+00:00
        2018-08-17 00:03:00+00:00  binance::BTC_USDT  6294.520000  34.611797       BTC_USDT     binance 2018-08-17 00:02:00+00:00
        ...
        2018-08-17 00:04:00+00:00   kucoin::ETH_USDT   285.884638   0.074655       ETH_USDT      kucoin 2018-08-17 00:03:00+00:00
        2018-08-17 00:05:00+00:00  binance::BTC_USDT  6294.990000  18.986206       BTC_USDT     binance 2018-08-17 00:04:00+00:00
        2018-08-17 00:05:00+00:00   kucoin::ETH_USDT   285.884637   0.006141       ETH_USDT      kucoin 2018-08-17 00:04:00+00:00
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
        # Initialize the `MarketDataInterface`.
        multiple_symbols_client = self._helper()
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        market_data_client = mdmadacl.MarketDataInterface(
            "full_symbol",
            full_symbols,
            "start_ts",
            "end_ts",
            [],
            hdateti.get_current_time,
            im_client=multiple_symbols_client,
        )
        # Read data.
        start_ts = pd.Timestamp("2018-08-17T00:01:00")
        end_ts = pd.Timestamp("2018-08-17T00:05:00")
        data = market_data_client.get_data_for_interval(
            start_ts,
            end_ts,
            "end_ts",
            full_symbols,
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
        df.columns=full_symbol,open,high,low,close,volume,epoch,currency_pair,exchange_id
        df.shape=(8, 9)
                                         full_symbol     open     high      low    close     volume          epoch currency_pair exchange_id
        2018-08-17 00:01:00+00:00  binance::BTC_USDT  6311.64  6311.77  6302.81  6302.81  16.781206  1534464060000      BTC_USDT     binance
        2018-08-17 00:02:00+00:00  binance::BTC_USDT  6302.81  6306.00  6292.79  6297.26  55.373226  1534464120000      BTC_USDT     binance
        2018-08-17 00:03:00+00:00  binance::BTC_USDT  6299.97  6299.97  6286.93  6294.52  34.611797  1534464180000      BTC_USDT     binance
        ...
        2018-08-17 00:02:00+00:00  kucoin::ETH_USDT  286.405988  286.405988  285.400193  285.400197  0.162255  1534464120000      ETH_USDT      kucoin
        2018-08-17 00:03:00+00:00  kucoin::ETH_USDT  285.400193  285.400193  285.400193  285.400193  0.020260  1534464180000      ETH_USDT      kucoin
        2018-08-17 00:04:00+00:00  kucoin::ETH_USDT  285.400193  285.884638  285.400193  285.884638  0.074655  1534464240000      ETH_USDT      kucoin
        """
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
        # Initialize the `MarketDataInterface`.
        multiple_symbols_client = self._helper()
        full_symbols = ["binance::BTC_USDT"]
        market_data_client = mdmadacl.MarketDataInterface(
            "full_symbol",
            full_symbols,
            "start_ts",
            "end_ts",
            [],
            hdateti.get_current_time,
            im_client=multiple_symbols_client,
        )
        # Compute TWAP price.
        start_ts = pd.Timestamp("2018-08-17T00:01:00")
        end_ts = pd.Timestamp("2018-08-17T00:05:00")
        actual = market_data_client.get_twap_price(
            start_ts,
            end_ts,
            "end_ts",
            full_symbols[0],
            "close",
        ).round(2)
        self.assertEqual(actual, 6295.72)

    def test_should_be_online1(self) -> None:
        """
        Test that the interface is available at the given time.
        """
        # Initialize the `MarketDataInterface`.
        multiple_symbols_client = self._helper()
        full_symbols = ["binance::BTC_USDT"]
        market_data_client = mdmadacl.MarketDataInterface(
            "full_symbol",
            full_symbols,
            "start_ts",
            "end_ts",
            [],
            hdateti.get_current_time,
            im_client=multiple_symbols_client,
        )
        # Conduct the check.
        wall_clock_time = pd.Timestamp("2018-08-17T00:01:00")
        actual = market_data_client.should_be_online(wall_clock_time)
        self.assertEqual(actual, True)

    def test_get_last_end_time1(self) -> None:
        """
        Test that a call for the last end time is causing an error for now.
        """
        # Initialize the `MarketDataInterface`.
        multiple_symbols_client = self._helper()
        full_symbols = ["binance::BTC_USDT"]
        market_data_client = mdmadacl.MarketDataInterface(
            "full_symbol",
            full_symbols,
            "start_ts",
            "end_ts",
            [],
            hdateti.get_current_time,
            im_client=multiple_symbols_client,
        )
        actual = market_data_client._get_last_end_time()
        self.assertEqual(actual, NotImplementedError)

    @staticmethod
    def _helper() -> imvcdcli.MultipleSymbolsImClient:
        """
        Get `MultipleSymbolsImClient` object for the tests.
        """
        # Get path to the dir with test data.
        test_dir = os.path.join(
            hgit.get_client_root(False),
            "im_v2/ccxt/data/client/test/test_data",
        )
        # Initialize clients.
        ccxt_file_client = imvcdclcl.CcxtCsvFileSystemClient(
            data_type="ohlcv", root_dir=test_dir
        )
        multiple_symbols_client = imvcdcli.MultipleSymbolsImClient(
            ccxt_file_client, "concat"
        )
        return multiple_symbols_client
