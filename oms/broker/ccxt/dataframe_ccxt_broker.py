"""
Import as:

import oms.broker.ccxt.dataframe_ccxt_broker as obcdccbr
"""

import logging
from typing import Any, Dict, Optional

import pandas as pd

import oms.broker.broker as obrobrok
import oms.broker.ccxt.ccxt_utils as obccccut

_LOG = logging.getLogger(__name__)


# #############################################################################
# DataFrameCcxtBroker
# #############################################################################


# TODO(gp): P0, @all IMO it should be CcxtDataFrameBroker since this is the
# CCXT version of a DataFrameBroker. Also change the name of the file.
class DataFrameCcxtBroker(obrobrok.DataFrameBroker):
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
) -> obrobrok.DataFrameBroker:
    market_info = obccccut.load_market_data_info()
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
