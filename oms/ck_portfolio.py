"""
Import as:

import oms.ck_portfolio as ockport
"""

import logging
from typing import Any, Dict, List, Optional

import market_data as mdata
import oms.ccxt_broker as occxbrok
import oms.portfolio as omportfo

_LOG = logging.getLogger(__name__)


class CkPortfolio(omportfo.DataFramePortfolio):
    """
    Portfolio class connected to CK OMS.

    Invariant: this class should have minimal state, but always query the DB.
    """

    def __init__(
        self,
        *args: Any,
        # table_name: str,
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
            **kwargs,
        )


def get_CcxtPortfolio_prod_instance(
    strategy_id: str,
    liveness: str,
    instance_type: str,
    market_data: mdata.MarketData,
    asset_ids: Optional[List[int]],
    order_duration_in_mins: int,
    order_extra_params: Optional[Dict[str, Any]],
    pricing_method: str,
) -> CkPortfolio:
    """
    Build an CK Portfolio retrieving its state from the DB.
    """
    # Build CkBroker.
    broker = occxbrok.get_CcxtBroker_prod_instance1(
        market_data,
        strategy_id,
        liveness,
        instance_type,
        order_duration_in_mins,
        order_extra_params,
    )
    # Build CkPortfolio.
    mark_to_market_col = "close"
    initial_cash = 1e6
    portfolio = CkPortfolio.from_cash(
        broker,
        mark_to_market_col,
        pricing_method,
        initial_cash=initial_cash,
        asset_ids=asset_ids,
    )
    return portfolio
