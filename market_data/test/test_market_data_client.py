import os

import pandas as pd

import helpers.datetime_ as hdateti
import helpers.git as hgit
import helpers.printing as hprint
import helpers.unit_test as hunitest
import im_v2.ccxt.data.client.clients as imvcdclcl
import im_v2.common.data.client as imvcdcli
import market_data.market_data_client as mdmadacl


# TODO(Grisha): add more tests.
class TestGetDataForInterval(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that `MarketDataInterface` returns data for interval correctly.
        """
        # Initialize the `IM` client.
        test_dir = os.path.join(
            hgit.get_client_root(False),
            "im_v2/ccxt/data/client/test/test_data",
        )
        ccxt_file_client = imvcdclcl.CcxtCsvFileSystemClient(
            data_type="ohlcv", root_dir=test_dir
        )
        multiple_symbols_client = imvcdcli.MultipleSymbolsImClient(
            ccxt_file_client, "concat"
        )
        # Initialize the `MarketDataInterface`.
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
