"""
Import as:

import market_data.market_data_client_example as mdmdclex
"""

from typing import Dict, List

import helpers.hdatetime as hdateti
import im_v2.ccxt.data.client.ccxt_clients_example as imvcdcccex
import market_data.market_data_im_client as mdmdimcl


# TODO(gp): Merge into market_data_example.py
# TODO(gp): -> MarketDataImClient
def get_MarketDataInterface_example1(
    asset_ids: List[str], columns: List[str], column_remap: Dict[str, str]
) -> mdmdimcl.MarketDataInterface:
    ccxt_client = imvcdcccex.get_CcxtCsvClient_example1()
    #
    asset_id_col = "full_symbol"
    start_time_col_name = "start_ts"
    end_time_col_name = "end_ts"
    get_wall_clock_time = hdateti.get_current_time
    market_data_client = mdmdimcl.MarketDataInterface(
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
