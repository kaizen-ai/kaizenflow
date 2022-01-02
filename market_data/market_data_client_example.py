"""
Import as:

import market_data.market_data_client_example as mdmdclex
"""

import os
from typing import Dict, List

import pandas as pd

import helpers.datetime_ as hdateti
import helpers.git as hgit
import helpers.printing as hprint
import helpers.unit_test as hunitest
import im_v2.ccxt.data.client.ccx_clients_example as imvcdcccex
import im_v2.ccxt.data.client.clients as imvcdclcl
import im_v2.common.data.client as imvcdcli
import market_data.market_data_client as mdmadacl


def get_MarketDataInterface_example1(
    asset_ids: List[str], columns: List[str], column_remap: Dict[str, str]
) -> mdmadacl.MarketDataInterface:
    ccxt_client = imvcdcccex.get_CcxtCsvFileSytemClient_example1()
    #
    asset_id_col = "full_symbol"
    start_time_col_name = "start_ts"
    end_time_col_name = "end_ts"
    get_wall_clock_time = hdateti.get_current_time
    market_data_client = mdmadacl.MarketDataInterface(
        asset_id_col,
        asset_ids,
        start_time_col_name,
        end_time_col_name,
        columns,
        get_wall_clock_time,
        im_client=ccxt_client,
        column_remap=column_remap,
    )
    return market_data_client
