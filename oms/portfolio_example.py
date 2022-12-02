"""
Import as:

import oms.portfolio_example as oporexam
"""

import asyncio
import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

import helpers.hsql as hsql
import market_data as mdata
import oms.broker as ombroker
import oms.broker_example as obroexam
import oms.ccxt_broker as occxbrok
import oms.oms_db as oomsdb
import oms.portfolio as omportfo

_LOG = logging.getLogger(__name__)


# #############################################################################
# DataFramePortfolio
# #############################################################################


def get_DataFramePortfolio_example1(
    event_loop: Optional[asyncio.AbstractEventLoop],
    *,
    market_data: Optional[mdata.MarketData] = None,
    mark_to_market_col: str = "price",
    pricing_method: str = "last",
    timestamp_col: str = "end_datetime",
    asset_ids: Optional[List[int]] = None,
) -> omportfo.DataFramePortfolio:
    """
    Contain:
    - a `DataFramePortfolio` (i.e., a portfolio backed by a dataframe to keep
      track of the state)
    - a `DataFrameBroker` (i.e., a broker that executes the orders immediately)
    """
    # Build a DataFrameBroker.
    broker = obroexam.get_DataFrameBroker_example1(
        event_loop,
        market_data=market_data,
        timestamp_col=timestamp_col,
    )
    # Build a DataFramePortfolio.
    mark_to_market_col = mark_to_market_col
    initial_cash = 1e6
    portfolio = omportfo.DataFramePortfolio.from_cash(
        broker,
        mark_to_market_col,
        pricing_method,
        #
        initial_cash=initial_cash,
        asset_ids=asset_ids,
    )
    return portfolio


def get_DataFramePortfolio_example2(
    strategy_id: str,
    account: str,
    market_data: mdata.MarketData,
    timestamp_col: str,
    mark_to_market_col: str,
    pricing_method: str,
    initial_holdings: pd.Series,
    *,
    column_remap: Optional[Dict[str, str]] = None,
) -> omportfo.DataFramePortfolio:
    """
    Contain:
    - a `DataFramePortfolio` (i.e., a portfolio backed by a dataframe to keep
      track of the state)
    - a `DataFrameBroker` (i.e., a broker that executes the orders immediately)

    exposing all the parameters for creating these objects.
    """
    # Build DataFrameBroker.
    broker = ombroker.DataFrameBroker(
        strategy_id,
        market_data,
        account=account,
        timestamp_col=timestamp_col,
        column_remap=column_remap,
    )
    # Build DataFramePortfolio.
    portfolio = omportfo.DataFramePortfolio(
        broker,
        mark_to_market_col,
        pricing_method,
        initial_holdings,
    )
    return portfolio


# TODO(Grisha): @Dan Combine with other examples.
def get_DataFramePortfolio_example3(
    *,
    market_data: Optional[mdata.MarketData] = None,
    asset_ids: Optional[List[int]] = None,
) -> omportfo.DataFramePortfolio:
    """
    Contain:
    - a `DataFramePortfolio` (i.e., a portfolio backed by a dataframe to keep
      track of the state)
    - a `DataFrameBroker` for prod (i.e., a broker that executes the orders immediately)
    """
    # Build a DataFrameBroker.
    broker = occxbrok.get_SimulatedCcxtBroker_instance1(market_data)
    # TODO(Grisha): @Dan Pass parameters via config.
    # Build a DataFramePortfolio.
    initial_cash = 700
    mark_to_market_col = "close"
    trading_period_str = "5T"
    pricing_method = "twap." + trading_period_str
    portfolio = omportfo.DataFramePortfolio.from_cash(
        broker,
        mark_to_market_col,
        pricing_method,
        #
        initial_cash=initial_cash,
        asset_ids=asset_ids,
    )
    return portfolio


# #############################################################################
# DatabasePortfolio
# #############################################################################


# TODO(gp): Remove the repetition across these functions.
def get_DatabasePortfolio_example1(
    event_loop: Optional[asyncio.AbstractEventLoop],
    db_connection: hsql.DbConnection,
    # TODO(gp): For symmetry with get_DatabaseBroker_example1 we should have a
    #  default value.
    table_name: str,
    *,
    market_data: Optional[mdata.MarketData] = None,
    mark_to_market_col: str = "price",
    pricing_method: str = "last",
    timestamp_col: str = "end_datetime",
    asset_ids: Optional[List[int]] = None,
    broker_log_dir: Optional[str] = None,
) -> omportfo.DatabasePortfolio:
    """
    Contain:
    - a `DatabasePortfolio`
    - a `DatabaseBroker`
    """
    # Build DatabaseBroker.
    broker = obroexam.get_DatabaseBroker_example1(
        event_loop,
        db_connection,
        market_data=market_data,
        log_dir=broker_log_dir,
        timestamp_col=timestamp_col,
    )
    # Build DatabasePortfolio.
    initial_cash = 1e6
    portfolio = omportfo.DatabasePortfolio.from_cash(
        broker,
        mark_to_market_col,
        pricing_method,
        table_name=table_name,
        #
        initial_cash=initial_cash,
        asset_ids=asset_ids,
    )
    return portfolio


def get_DatabasePortfolio_example2(
    event_loop: Optional[asyncio.AbstractEventLoop],
    db_connection: hsql.DbConnection,
    # TODO(gp): For symmetry with get_DatabaseBroker_example1 we should have a
    #  default value.
    table_name: str,
    universe: List[int],
    *,
    market_data: Optional[mdata.MarketData] = None,
    mark_to_market_col: str = "price",
    pricing_method: str = "last",
    timestamp_col: str = "end_datetime",
) -> omportfo.DatabasePortfolio:
    """
    Contain:
    - a `DatabasePortfolio`, which is initialized from the database.
    - a `DatabaseBroker`
    """
    # Build DatabaseBroker.
    broker = obroexam.get_DatabaseBroker_example1(
        event_loop,
        db_connection,
        market_data=market_data,
        timestamp_col=timestamp_col,
    )
    # Build DatabasePortfolio.
    initial_holdings = pd.Series(np.nan, universe)
    portfolio = omportfo.DatabasePortfolio(
        broker,
        mark_to_market_col,
        pricing_method,
        initial_holdings,
        table_name=table_name,
        retrieve_initial_holdings_from_db=True,
    )
    return portfolio


# TODO(gp): It's specific of IG move to lime.
def get_DatabasePortfolio_example3(
    db_connection: hsql.DbConnection,
    event_loop: asyncio.AbstractEventLoop,
    market_data: mdata.MarketData,
    asset_ids: List[int],
    *,
    broker_log_dir: Optional[str] = None,
    # TODO(gp): Return oms.Portfolio like parent class?
) -> omportfo.DatabasePortfolio:
    """
    Contain:
    - a DatabaseBroker
    - a DatabasePortfolio
    """
    table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
    timestamp_col = "end_time"
    portfolio = get_DatabasePortfolio_example1(
        event_loop,
        db_connection,
        table_name,
        market_data=market_data,
        mark_to_market_col="close",
        pricing_method="twap.5T",
        timestamp_col=timestamp_col,
        asset_ids=asset_ids,
        broker_log_dir=broker_log_dir,
    )
    portfolio.broker._column_remap = {
        "bid": "bid",
        "ask": "ask",
        "midpoint": "midpoint",
        "price": "close",
    }
    return portfolio


# TODO(gp): This is more general than ..._example4.
def get_DatabasePortfolio_example4(
    db_connection: hsql.DbConnection,
    event_loop: asyncio.AbstractEventLoop,
    market_data: mdata.MarketData,
    asset_ids: List[int],
    pricing_method: str,
) -> omportfo.DatabasePortfolio:
    """
    This is used to simulated a E8 prod system.

    Contain:
    - a DatabaseBroker
    - a DatabasePortfolio
    """
    table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
    timestamp_col = "end_time"
    portfolio = get_DatabasePortfolio_example1(
        event_loop,
        db_connection,
        table_name,
        market_data=market_data,
        mark_to_market_col="close",
        # pricing_method="twap.5T",
        pricing_method=pricing_method,
        timestamp_col=timestamp_col,
        asset_ids=asset_ids,
    )
    portfolio.broker._column_remap = {
        "bid": "bid",
        "ask": "ask",
        "midpoint": "midpoint",
        "price": "close",
    }
    return portfolio