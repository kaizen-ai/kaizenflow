"""
Import as:

import oms_lime.eg_portfolio as oliegpor
"""

import logging
from typing import Any

import helpers.hdbg as hdbg
import asyncio
from typing import Any, List, Optional, Dict

import numpy as np
import pandas as pd

import market_data as mdata
import oms.ck_credentials as omsckc
import oms_lime.eg_broker_example as olegbrex
import oms_lime.eg_portfolio as oliegpor
import vendors_lime.eg_credentials as vliegcre



_LOG = logging.getLogger(__name__)


class CkPortfolio(oms.DatabasePortfolio):
    """
    Portfolio class connected to CK OMS.

    Invariant: this class should have minimal state, but always query the DB.
    """

    def __init__(
        self,
        *args: Any,
        table_name: str,
        **kwargs: Any,
    ):
        """
        Constructor.
        """
        # In `CkPortfolio` the name of the table depends on the type of account,
        # which depends on the Broker. Thus, we need to initialize the parent class
        # with Broker and then overwrite the name of the table.
        super().__init__(
            *args,
            table_name=table_name,
            **kwargs,
        )
        # TODO(gp): @all use oms.CURRENT_POSITIONS_TABLE_NAME
        #table_name = "current_positions"
        table_name = omsckc.get_core_db_view(
            "current_positions", liveness, instance_type
        )
        hdbg.dassert_eq(table_name, self._table_name)


# TODO(gp): Factor out common code.
def get_CkPortfolio_prod_instance(
        strategy_id: str,
        liveness: str,
        instance_type: str,
        retrieve_initial_holdings_from_db: bool,
        market_data: mdata.MarketData,
        asset_ids: Optional[List[int]],
        order_duration_in_mins: int,
        order_extra_params: Optional[Dict[str, Any]],
        pricing_method: str,
) -> oliegpor.CkPortfolio:
    """
    Build an CK Portfolio retrieving its state from the DB.
    """
    # Build CkBroker.
    broker = olegbrex.get_CcxtBroker_prod_instance1(
        market_data,
        strategy_id,
        liveness,
        instance_type,
        order_duration_in_mins,
        order_extra_params,
    )
    # Build CkPortfolio.
    mark_to_market_col = "close"
    # timestamp_col = "end_time"
    if retrieve_initial_holdings_from_db:
        # Use last state from DB.
        initial_holdings = pd.Series(
            np.nan, asset_ids + [CkPortfolio.CASH_ID]
        )
    else:
        # Restart from scratch.
        initial_holdings = pd.Series(
            0, asset_ids + [CkPortfolio.CASH_ID]
        )
    # In `CkPortfolio` the name of the table depends on the type of account,
    # e.g., "current_positions_candidate_view".
    table_name = omsckc.get_core_db_view(
        "current_positions", liveness, instance_type
    )
    portfolio = oliegpor.CkPortfolio(
        broker,
        mark_to_market_col,
        pricing_method,
        initial_holdings=initial_holdings,
        retrieve_initial_holdings_from_db=retrieve_initial_holdings_from_db,
        table_name=table_name,
    )
    return portfolio

