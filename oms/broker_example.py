"""
Import as:

import oms.broker_example as obroexam
"""

import asyncio

import helpers.sql as hsql
import market_data.market_data_interface_example as mdmdinex
import oms.broker as ombroker
import oms.oms_db as oomsdb


def get_broker_example1(event_loop: asyncio.AbstractEventLoop) -> ombroker.Broker:
    # Build the market data interface.
    (
        market_data_interface,
        get_wall_clock_time,
    ) = mdmdinex.get_replayed_time_market_data_interface_example2(event_loop)
    # Build the broker.
    broker = ombroker.Broker(market_data_interface, get_wall_clock_time)
    return broker


def get_mocked_broker_example1(
    event_loop: asyncio.AbstractEventLoop,
    db_connection: hsql.DbConnection,
    *,
    submitted_orders_table_name: str = oomsdb.SUBMITTED_ORDERS_TABLE_NAME,
    accepted_orders_table_name: str = oomsdb.ACCEPTED_ORDERS_TABLE_NAME,
) -> ombroker.Broker:
    # Build the market data interface.
    (
        market_data_interface,
        get_wall_clock_time,
    ) = mdmdinex.get_replayed_time_market_data_interface_example2(event_loop)
    # Build the broker.
    broker = ombroker.MockedBroker(market_data_interface, get_wall_clock_time)
    return broker
