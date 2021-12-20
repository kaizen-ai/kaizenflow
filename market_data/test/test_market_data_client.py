import os

import pandas as pd

import helpers.datetime_ as hdateti
import helpers.git as hgit
import helpers.unit_test as hunitest
import im_v2.ccxt.data.client.clients as imvcdclcl
import im_v2.common.data.client as imvcdcli
import market_data.market_data_client as mclient

_LOCAL_ROOT_DIR = os.path.join(
    hgit.get_client_root(False),
    "im_v2/ccxt/data/client/test/test_data",
)


class TestGetData(hunitest.TestCase):
    def test1(self):
        ccxt_file_client = imvcdclcl.CcxtCsvFileSystemClient(
            data_type="ohlcv", root_dir=_LOCAL_ROOT_DIR
        )
        full_symbols = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
        multiple_symbols_client = imvcdcli.MultipleSymbolsImClient(ccxt_file_client, "concat")
        market_data_client = mclient.MarketDataInterFace(
            "full_symbol",
            full_symbols,
            "start_timestamp_col",
            "end_timestamp_col",
            hdateti.GetWallClockTime,
            im_client=multiple_symbols_client,
        )
        data = market_data_client._get_data(
            pd.Timestamp("2018-08-17T00:01:00"),
            pd.Timestamp("2018-08-17T00:05:00"),
            "end_timestamp_col",
            full_symbols,
            left_close=True,
            right_close=False,
            normalize_data=True,
            limit=None,
        )
        print(data)