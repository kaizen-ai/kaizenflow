"""
Import as:

import oms.ccxt.dataframe_ccxt_broker as ocdaccbr
"""

import logging
from typing import Any, Dict, Optional

import pandas as pd

import oms.broker as ombroker
import oms.ccxt.ccxt_utils as occccuti

_LOG = logging.getLogger(__name__)


# #############################################################################
# DataFrameCcxtBroker
# #############################################################################


# TODO(gp): IMO it should be CcxtDataFrameBroker
class DataFrameCcxtBroker(ombroker.DataFrameBroker):
    def __init__(
        self,
        *args: Any,
        stage: str,
        market_info: Dict[int, float],
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.stage = stage
        self.market_info = market_info


def get_DataFrameCcxtBroker_instance1(
    strategy_id: str,
    market_data: pd.DataFrame,
    universe_version: str,
    stage: str,
    *,
    column_remap: Optional[Dict[str, str]] = None,
) -> ombroker.DataFrameBroker:
    market_info = occccuti.load_market_data_info()
    broker = DataFrameCcxtBroker(
        strategy_id,
        market_data,
        universe_version,
        stage,
        column_remap=column_remap,
        stage=stage,
        market_info=market_info,
    )
    return broker
