"""
Import as:

import oms.broker_example as obroexam
"""

import asyncio
from typing import Dict, Optional

import helpers.hsql as hsql
import market_data as mdata
import oms.broker as ombroker
import oms.oms_db as oomsdb


def get_DataFrameBroker_example1(
    event_loop: Optional[asyncio.AbstractEventLoop],
    *,
    market_data: Optional[mdata.MarketData] = None,
    timestamp_col: str = "end_datetime",
    column_remap: Optional[Dict[str, str]] = None,
) -> ombroker.DataFrameBroker:
    """
    Build a `DataFrameBroker` using a `MarketData`, unless specified.
    """
    # Build MarketData, if needed.
    if market_data is None:
        (
            market_data,
            _,
        ) = mdata.get_ReplayedTimeMarketData_example3(event_loop)
    # Build DataFrameBroker.
    strategy_id = "SAU1"
    account = "candidate"
    broker = ombroker.DataFrameBroker(
        strategy_id,
        market_data,
        account=account,
        timestamp_col=timestamp_col,
        column_remap=column_remap,
    )
    return broker


def get_DatabaseBroker_example1(
    event_loop: Optional[asyncio.AbstractEventLoop],
    db_connection: hsql.DbConnection,
    *,
    market_data: Optional[mdata.MarketData] = None,
    timestamp_col: str = "end_datetime",
    submitted_orders_table_name: str = oomsdb.SUBMITTED_ORDERS_TABLE_NAME,
    accepted_orders_table_name: str = oomsdb.ACCEPTED_ORDERS_TABLE_NAME,
    log_dir: Optional[str] = None,
) -> ombroker.DatabaseBroker:
    """
    Build a `DatabaseBroker` using `MarketData`, unless specified.
    """
    # Build MarketData, if needed.
    if market_data is None:
        (
            market_data,
            _,
        ) = mdata.get_ReplayedTimeMarketData_example3(event_loop)
    # Build DatabaseBroker.
    strategy_id = "SAU1"
    account = "candidate"
    broker = ombroker.DatabaseBroker(
        strategy_id,
        market_data,
        account=account,
        timestamp_col=timestamp_col,
        db_connection=db_connection,
        submitted_orders_table_name=submitted_orders_table_name,
        accepted_orders_table_name=accepted_orders_table_name,
        log_dir=log_dir,
    )
    return broker