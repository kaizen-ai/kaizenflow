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
    # Build SimulatedBroker.
    broker = obroexam.get_simulated_broker_example1(
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
    Expose all parameters for creating a `DataFramePortfolio`.
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


# TODO(gp): -> get_MockedPortfolio_example
def get_mocked_portfolio_example1(
    event_loop: Optional[asyncio.AbstractEventLoop],
    db_connection: hsql.DbConnection,
    # TODO(gp): For symmetry with get_mocked_broker_example1 we should have a
    #  default value.
    table_name: str,
    *,
    market_data: Optional[mdata.MarketData] = None,
    mark_to_market_col: str = "price",
    pricing_method: str = "last",
    timestamp_col: str = "end_datetime",
    asset_ids: Optional[List[int]] = None,
) -> omportfo.MockedPortfolio:
    # Build MockedBroker.
    broker = obroexam.get_mocked_broker_example1(
        event_loop,
        db_connection,
        market_data=market_data,
        timestamp_col=timestamp_col,
    )
    # Build MockedPortfolio.
    initial_cash = 1e6
    portfolio = omportfo.MockedPortfolio.from_cash(
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


def get_MockedPortfolio_example2(
    event_loop: Optional[asyncio.AbstractEventLoop],
    db_connection: hsql.DbConnection,
    # TODO(gp): For symmetry with get_mocked_broker_example1 we should have a
    #  default value.
    table_name: str,
    universe: List[int],
    *,
    market_data: Optional[mdata.MarketData] = None,
    mark_to_market_col: str = "price",
    pricing_method: str = "last",
    timestamp_col: str = "end_datetime",
) -> omportfo.MockedPortfolio:
    """
    Get a Portfolio initialized from the database.
    """
    # Build MockedBroker.
    broker = obroexam.get_mocked_broker_example1(
        event_loop,
        db_connection,
        market_data=market_data,
        timestamp_col=timestamp_col,
    )
    # Build MockedPortfolio.
    initial_holdings = pd.Series(np.nan, universe)
    portfolio = omportfo.MockedPortfolio(
        broker,
        mark_to_market_col,
        pricing_method,
        initial_holdings,
        db_connection=db_connection,
        table_name=table_name,
        retrieve_initial_holdings_from_db=True,
    )
    return portfolio
