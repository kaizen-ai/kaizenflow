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
import oms.portfolio as omportfo
import oms.oms_db as oomsdb

_LOG = logging.getLogger(__name__)


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
    - a SimulatedBroker (i.e., a broker that executes the orders immediately)
    - a DataFramePortfolio (i.e., a portfolio backed by a dataframe to keep
      track of the state)
    """
    # Build SimulatedBroker.
    broker = obroexam.get_SimulatedBroker_example1(
        event_loop,
        market_data=market_data,
        timestamp_col=timestamp_col,
    )
    # Build DataFramePortfolio.
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
    - a SimulatedBroker (i.e., a broker that executes the orders immediately)
    - a DataFramePortfolio (i.e., a portfolio backed by a dataframe to keep
      track of the state)

    exposing all the parameters for creating these objects.
    """
    # Build SimulatedBroker.
    broker = ombroker.SimulatedBroker(
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


# #################################################################################


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
) -> omportfo.DatabasePortfolio:
    """
    Contain:
    - a DatabaseBroker
    - a DatabasePortfolio
    """
    # Build DatabaseBroker.
    broker = obroexam.get_DatabaseBroker_example1(
        event_loop,
        db_connection,
        market_data=market_data,
        timestamp_col=timestamp_col,
    )
    # Build DatabasePortfolio.
    initial_cash = 1e6
    portfolio = omportfo.DatabasePortfolio.from_cash(
        broker,
        mark_to_market_col,
        pricing_method,
        db_connection=db_connection,
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
    - a DatabaseBroker
    - a DatabasePortfolio, which is initialized from the database.
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
        db_connection=db_connection,
        table_name=table_name,
        retrieve_initial_holdings_from_db=True,
    )
    return portfolio


def get_DatabasePortfolio_example3(
    db_connection: hsql.DbConnection,
    event_loop: asyncio.AbstractEventLoop,
    market_data: mdata.MarketData,
    # TODO(gp): Return oms.Portfolio like parent class?
) -> omportfo.DatabasePortfolio:
    """
    Contain:
    - a DatabaseBroker
    - a DatabasePortfolio

    configured to use 3 asset ids.
    """
    table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
    # Neither the fake data nor the pipeline is filtering out weekends, and
    # so this is treated as a valid trading day.
    asset_ids = [
        17085,
        # 13684,
        # 10971
    ]
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
    )
    portfolio.broker._column_remap = {
        "bid": "bid",
        "ask": "ask",
        "midpoint": "midpoint",
        "price": "close",
    }
    return portfolio
