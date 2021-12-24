"""
Import as:

import oms.portfolio_example as oporexam
"""

import asyncio
import logging
from typing import Optional

import pandas as pd

import helpers.sql as hsql
import market_data.market_data_interface as mdmadain
import oms.broker_example as obroexam
import oms.portfolio as omportfo

_LOG = logging.getLogger(__name__)


def get_simulated_portfolio_example1(
    event_loop: Optional[asyncio.AbstractEventLoop],
    initial_timestamp: pd.Timestamp,
    *,
    market_data_interface: Optional[mdmadain.AbstractMarketDataInterface] = None,
) -> omportfo.SimulatedPortfolio:
    # Build SimulatedBroker.
    broker = obroexam.get_simulated_broker_example1(
        event_loop, market_data_interface=market_data_interface
    )
    market_data_interface = broker.market_data_interface
    # Build SimulatedPortfolio.
    strategy_id = "st1"
    account = "paper"
    get_wall_clock_time = market_data_interface.get_wall_clock_time
    asset_id_column = "asset_id"
    mark_to_market_col = "price"
    timestamp_col = "end_datetime"
    initial_cash = 1e6
    portfolio = omportfo.SimulatedPortfolio.from_cash(
        strategy_id,
        account,
        market_data_interface,
        get_wall_clock_time,
        asset_id_column,
        mark_to_market_col,
        timestamp_col,
        broker=broker,
        #
        initial_cash=initial_cash,
        initial_timestamp=initial_timestamp,
    )
    return portfolio


def get_mocked_portfolio_example1(
    event_loop: Optional[asyncio.AbstractEventLoop],
    db_connection: hsql.DbConnection,
    # TODO(gp): For symmetry with get_mocked_broker_example1 we should have a
    #  default value.
    table_name: str,
    initial_timestamp: pd.Timestamp,
    *,
    market_data_interface: Optional[mdmadain.AbstractMarketDataInterface] = None,
    mark_to_market_col: str = "price",
) -> omportfo.MockedPortfolio:
    # Build MockedBroker.
    broker = obroexam.get_mocked_broker_example1(
        event_loop, db_connection, market_data_interface=market_data_interface
    )
    # Build MockedPortfolio.
    strategy_id = "st1"
    account = "candidate"
    market_data_interface = broker.market_data_interface
    get_wall_clock_time = market_data_interface.get_wall_clock_time
    asset_id_column = "asset_id"
    timestamp_col = "end_datetime"
    initial_cash = 1e6
    portfolio = omportfo.MockedPortfolio.from_cash(
        strategy_id,
        account,
        market_data_interface,
        get_wall_clock_time,
        asset_id_column,
        mark_to_market_col,
        timestamp_col,
        broker=broker,
        db_connection=db_connection,
        table_name=table_name,
        asset_id_col=asset_id_column,
        #
        initial_cash=initial_cash,
        initial_timestamp=initial_timestamp,
    )
    return portfolio
