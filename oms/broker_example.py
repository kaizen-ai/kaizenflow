"""
Import as:

import oms.broker_example as obroexam
"""

import asyncio
from typing import Optional

import helpers.sql as hsql
import market_data.market_data_interface as mdmadain
import market_data.market_data_interface_example as mdmdinex
import oms.broker as ombroker
import oms.oms_db as oomsdb


def get_simulated_broker_example1(
    event_loop: Optional[asyncio.AbstractEventLoop],
    *,
    market_data_interface: Optional[mdmadain.AbstractMarketDataInterface] = None,
) -> ombroker.SimulatedBroker:
    """
    Build an example of `SimulatedBroker` using an example `MarketDataInterface`,
    unless specified.
    """
    # Build MarketDataInterface.
    if market_data_interface is None:
        (
            market_data_interface,
            _,
        ) = mdmdinex.get_replayed_time_market_data_interface_example3(event_loop)
    # Build SimulatedBroker.
    strategy_id = "SAU1"
    account = "candidate"
    get_wall_clock_time = market_data_interface.get_wall_clock_time
    broker = ombroker.SimulatedBroker(
        strategy_id, account, market_data_interface, get_wall_clock_time
    )
    return broker


def get_mocked_broker_example1(
    event_loop: Optional[asyncio.AbstractEventLoop],
    db_connection: hsql.DbConnection,
    *,
    market_data_interface: Optional[mdmadain.AbstractMarketDataInterface] = None,
    submitted_orders_table_name: str = oomsdb.SUBMITTED_ORDERS_TABLE_NAME,
    accepted_orders_table_name: str = oomsdb.ACCEPTED_ORDERS_TABLE_NAME,
) -> ombroker.SimulatedBroker:
    """
    Build an example of `MockedBroker` using an example `MarketDataInterface`,
    unless specified.
    """
    # Build MarketDataInterface.
    if market_data_interface is None:
        (
            market_data_interface,
            _,
        ) = mdmdinex.get_replayed_time_market_data_interface_example3(event_loop)
    # Build MockedBroker.
    strategy_id = "SAU1"
    account = "candidate"
    get_wall_clock_time = market_data_interface.get_wall_clock_time
    broker = ombroker.MockedBroker(
        strategy_id,
        account,
        market_data_interface,
        get_wall_clock_time,
        db_connection=db_connection,
        submitted_orders_table_name=submitted_orders_table_name,
        accepted_orders_table_name=accepted_orders_table_name,
    )
    return broker
