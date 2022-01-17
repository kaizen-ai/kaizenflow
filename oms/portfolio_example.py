"""
Import as:

import oms.portfolio_example as oporexam
"""

import asyncio
import logging
from typing import List, Optional

import helpers.hsql as hsql
import market_data as mdata
import oms.broker_example as obroexam
import oms.portfolio as omportfo

_LOG = logging.getLogger(__name__)


def get_simulated_portfolio_example1(
    event_loop: Optional[asyncio.AbstractEventLoop],
    *,
    market_data: Optional[mdata.AbstractMarketData] = None,
    mark_to_market_col: str = "price",
    asset_ids: Optional[List[int]] = None,
) -> omportfo.SimulatedPortfolio:
    # Build SimulatedBroker.
    broker = obroexam.get_simulated_broker_example1(
        event_loop, market_data=market_data
    )
    # Build SimulatedPortfolio.
    strategy_id = "st1"
    account = "paper"
    asset_id_column = "asset_id"
    mark_to_market_col = mark_to_market_col
    timestamp_col = "end_datetime"
    initial_cash = 1e6
    portfolio = omportfo.SimulatedPortfolio.from_cash(
        strategy_id,
        account,
        broker,
        asset_id_column,
        mark_to_market_col,
        timestamp_col,
        #
        initial_cash=initial_cash,
        asset_ids=asset_ids,
    )
    return portfolio


def get_mocked_portfolio_example1(
    event_loop: Optional[asyncio.AbstractEventLoop],
    db_connection: hsql.DbConnection,
    # TODO(gp): For symmetry with get_mocked_broker_example1 we should have a
    #  default value.
    table_name: str,
    *,
    market_data: Optional[mdata.AbstractMarketData] = None,
    mark_to_market_col: str = "price",
    asset_ids: Optional[List[int]] = None,
) -> omportfo.MockedPortfolio:
    # Build MockedBroker.
    broker = obroexam.get_mocked_broker_example1(
        event_loop, db_connection, market_data=market_data
    )
    # Build MockedPortfolio.
    strategy_id = "st1"
    account = "candidate"
    asset_id_column = "asset_id"
    timestamp_col = "end_datetime"
    initial_cash = 1e6
    portfolio = omportfo.MockedPortfolio.from_cash(
        strategy_id,
        account,
        broker,
        asset_id_column,
        mark_to_market_col,
        timestamp_col,
        db_connection=db_connection,
        table_name=table_name,
        #
        initial_cash=initial_cash,
        asset_ids=asset_ids,
    )
    return portfolio
