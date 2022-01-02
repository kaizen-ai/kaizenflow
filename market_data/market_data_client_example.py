"""
Import as:

import market_data.market_data_client_example as mdmdc
"""

import os

import pandas as pd

import helpers.datetime_ as hdateti
import helpers.git as hgit
import helpers.printing as hprint
import helpers.unit_test as hunitest
import im_v2.ccxt.data.client.clients as imvcdclcl
import im_v2.common.data.client as imvcdcli
import im_v2.ccxt.data.client.ccx_clients_example as icdccce
import market_data.market_data_client as mdmadacl


def get_MarketDataInterface_example1() -> mdmadacl.MarketDataInterface:
    ccxt_client = self.get_CcxtCsvFileSytemClient_example1()
    #
    asset_id_col = "full_symbol"
    asset_ids = ["kucoin::ETH_USDT", "binance::BTC_USDT"]
    start_time_col_name = "start_ts"
    end_time_col_name = "end_ts"
    columns = []
    get_wall_clock_time = hdateti.get_current_time
    market_data_client = mdmadacl.MarketDataInterface(
        asset_id_col,
        asset_ids,
        start_time_col_name,
        end_time_col_name,
        columns,
        get_wall_clock_time,
        im_client=ccxt_client,
        column_remap={"full_symbol": "asset_id"},
    )
    return market_data_client
