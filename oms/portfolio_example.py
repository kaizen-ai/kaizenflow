"""
Import as:

import oms.portfolio_example as oporexam
"""

import logging

import pandas as pd

import market_data.market_data_interface as mdmadain
import oms.broker as ombroker
import oms.portfolio as omportfo

_LOG = logging.getLogger(__name__)


def get_portfolio_example1(
    market_data_interface: mdmadain.AbstractMarketDataInterface,
    initial_timestamp: pd.Timestamp,
) -> omportfo.Portfolio:
    strategy_id = "st1"
    account = "paper"
    get_wall_clock_time = market_data_interface.get_wall_clock_time
    asset_id_column = "asset_id"
    # price_column = "midpoint"
    mark_to_market_col = "price"
    timestamp_col = "end_datetime"
    broker = ombroker.Broker(market_data_interface, get_wall_clock_time)
    #
    initial_cash = 1e6
    portfolio = omportfo.Portfolio.from_cash(
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
