"""
Import as:

import market_data.market_data_instance as mdmadain
"""

import logging
from typing import List, Tuple

import helpers.hdatetime as hdateti
import im_v2.common.data.client as icdc
import market_data.real_time_market_data as mdrtmada
import market_data.replayed_market_data as mdremada

_LOG = logging.getLogger(__name__)


# #############################################################################


def get_RealTimeImClientMarketData_prod_instance1(
    im_client: icdc.ImClient,
    asset_ids: List[int],
) -> Tuple[mdremada.ReplayedMarketData, hdateti.GetWallClockTime]:
    """
    Build a `RealTimeMarketData` for production.
    """
    asset_id_col = "asset_id"
    start_time_col_name = "start_timestamp"
    end_time_col_name = "end_timestamp"
    columns = None
    event_loop = None
    get_wall_clock_time = lambda: hdateti.get_current_time(
        tz="ET", event_loop=event_loop
    )
    # Build a `ReplayedMarketData`.
    market_data = mdrtmada.RealTimeMarketData2(
        im_client,
        #
        asset_id_col,
        asset_ids,
        start_time_col_name,
        end_time_col_name,
        columns,
        get_wall_clock_time,
    )
    return market_data, get_wall_clock_time
