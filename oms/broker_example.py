"""
Import as:

import oms.broker_example as obroexam
"""

import asyncio
from typing import Optional

import helpers.hsql as hsql
import market_data as mdata
import oms.broker as ombroker
import oms.oms_db as oomsdb


def get_simulated_broker_example1(
    event_loop: Optional[asyncio.AbstractEventLoop],
    *,
    market_data: Optional[mdata.AbstractMarketData] = None,
) -> ombroker.SimulatedBroker:
    """
    Build an example of `SimulatedBroker` using an example `MarketData`, unless
    specified.
    """
    # Build MarketData.
    if market_data is None:
        (
            market_data,
            _,
        ) = mdata.get_ReplayedTimeMarketData_example3(event_loop)
    # Build SimulatedBroker.
    strategy_id = "SAU1"
    account = "candidate"
    broker = ombroker.SimulatedBroker(strategy_id, account, market_data)
    return broker


def get_mocked_broker_example1(
    event_loop: Optional[asyncio.AbstractEventLoop],
    db_connection: hsql.DbConnection,
    *,
    timestamp_col: str = "end_datetime",
    market_data: Optional[mdata.AbstractMarketData] = None,
    submitted_orders_table_name: str = oomsdb.SUBMITTED_ORDERS_TABLE_NAME,
    accepted_orders_table_name: str = oomsdb.ACCEPTED_ORDERS_TABLE_NAME,
) -> ombroker.SimulatedBroker:
    """
    Build an example of `MockedBroker` using an example `MarketData`, unless
    specified.
    """
    # Build MarketData.
    if market_data is None:
        (
            market_data,
            _,
        ) = mdata.get_ReplayedTimeMarketData_example3(event_loop)
    # Build MockedBroker.
    strategy_id = "SAU1"
    account = "candidate"
    broker = ombroker.MockedBroker(
        strategy_id,
        account,
        market_data,
        timestamp_col=timestamp_col,
        db_connection=db_connection,
        submitted_orders_table_name=submitted_orders_table_name,
        accepted_orders_table_name=accepted_orders_table_name,
    )
    return broker
