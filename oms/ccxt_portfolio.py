"""
Import as:

import oms.ccxt_portfolio as occxport
"""

import logging
from typing import Any, Dict, List, Optional

import market_data as mdata
import oms.ccxt_broker as occxbrok
import oms.hsecrets as omssec
import oms.portfolio as omportfo


_LOG = logging.getLogger(__name__)


class CcxtPortfolio(omportfo.DataFramePortfolio):
    """
    A Portfolio that stores the information in a dataframe backed by
    CcxtBroker.
    """

    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ):
        """
        Constructor.
        """
        super().__init__(
            *args,
            **kwargs,
        )


def get_CcxtPortfolio_prod_instance1(
    run_mode: str,
    strategy_id: str,
    market_data: mdata.MarketData,
    column_remap: Dict[str, str],
    universe_version: str,
    secret_identifier: omssec.SecretIdentifier,
    pricing_method: str,
    asset_ids: Optional[List[int]],
) -> CcxtPortfolio:
    """
    Initialize the `CcxtPortfolio` with cash using `CcxtBroker`.

    :param run_mode: see `_Cx_ProdSystem`
    :param strategy_id: see `Broker`
    :param market_data: see `Broker`
    :param universe_version: see `CcxtBroker`
    :param secret_identifier: see `CcxtBroker`
    :param pricing_method: see `Portfolio` ctor
    :param asset_ids: see `Portfolio.from_cash()`
    """
    # We prefer to configure code statically (e.g., without switches) but in this
    # case the prod Porfolio vs its paper-trading version are so close
    # (and we want to keep them close) that we use a switch.
    if run_mode == "prod":
        # Build `CcxtBroker` that is connected to the real exchange.
        broker = occxbrok.get_CcxtBroker_prod_instance1(
            strategy_id, market_data, universe_version, secret_identifier
        )
    elif run_mode == "paper_trading":
        _LOG.warning("Running the system with the `DataFrameCcxtBroker`")
        # Use the `DataFrameCcxtBroker`, i.e. no interaction with
        # the real exchange.
        stage = secret_identifier.stage
        broker = occxbrok.get_DataFrameCcxtBroker_instance1(
            strategy_id,
            market_data,
            column_remap,
            stage,
        )
    else:
        raise ValueError(f"Invalid run_mode='{self.run_mode}'")
    # Build CcxtPortfolio.
    mark_to_market_col = "close"
    initial_cash = 700
    portfolio = CcxtPortfolio.from_cash(
        broker,
        mark_to_market_col,
        pricing_method,
        initial_cash=initial_cash,
        asset_ids=asset_ids,
    )
    return portfolio