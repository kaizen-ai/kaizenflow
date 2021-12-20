import os

import pandas as pd

import helpers.datetime_ as hdateti
import helpers.git as hgit
import helpers.printing as hprint
import helpers.unit_test as hunitest
import im_v2.ccxt.data.client.clients as imvcdclcl
import im_v2.common.data.client as imvcdcli
import market_data.market_data_client as mclient


# TODO(Grisha): add more tests.
class TestGetData(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that `MarketDataInterface` returns data correctly.
        """
        # Initialize the `IM` client.
        test_dir = os.path.join(
            hgit.get_client_root(False),
            "im_v2/ccxt/data/client/test/test_data",
        )
        ccxt_file_client = imvcdclcl.CcxtCsvFileSystemClient(
            data_type="ohlcv", root_dir=test_dir
        )
        multiple_symbols_client = imvcdcli.MultipleSymbolsImClient(ccxt_file_client, "concat")
        # Initialize the `MarketDataInterface`.
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        market_data_client = mclient.MarketDataInterFace(
            "full_symbol",
            full_symbols,
            "start_ts",
            "end_ts",
            [],
            hdateti.GetWallClockTime,
            im_client=multiple_symbols_client,
        )
        # Read data.
        start_ts = pd.Timestamp("2018-08-17T00:01:00")
        end_ts = pd.Timestamp("2018-08-17T00:05:00")
        data = market_data_client._get_data(
            start_ts,
            end_ts,
            "end_ts",
            full_symbols,
            left_close=True,
            right_close=False,
            normalize_data=True,
            limit=None,
        )
        print(hprint.df_to_short_str("df", data))
