"""
Import as:

import oms.order_example as oordexam
"""

import logging

import pandas as pd

import market_data.market_data_interface_example as mdmdinex
import oms.order as omorder

_LOG = logging.getLogger(__name__)


def get_order_example1() -> omorder.Order:
    market_data_interface = None
    creation_timestamp = pd.Timestamp("2021-01-04 09:29:00-05:00")
    asset_id = 1
    type_ = "price@twap"
    start_timestamp = pd.Timestamp("2021-01-04 09:30:00-05:00")
    end_timestamp = pd.Timestamp("2021-01-04 09:35:00-05:00")
    num_shares = 100
    order_id = 0
    # Build Order.
    order = omorder.Order(
        market_data_interface,
        creation_timestamp,
        asset_id,
        type_,
        start_timestamp,
        end_timestamp,
        num_shares,
        order_id=order_id,
    )
    return order


def get_order_example2(event_loop) -> omorder.Order:
    (
        market_data_interface,
        _,
    ) = mdmdinex.get_replayed_time_market_data_interface_example2(event_loop)
    creation_timestamp = pd.Timestamp(
        "2000-01-01 09:29:00-05:00", tz="America/New_York"
    )
    asset_id = 101
    type_ = "price@twap"
    start_timestamp = pd.Timestamp(
        "2000-01-01 09:30:00-05:00", tz="America/New_York"
    )
    end_timestamp = pd.Timestamp(
        "2000-01-01 09:35:00-05:00", tz="America/New_York"
    )
    num_shares = 100
    order_id = 0
    # Build Order.
    order = omorder.Order(
        market_data_interface,
        creation_timestamp,
        asset_id,
        type_,
        start_timestamp,
        end_timestamp,
        num_shares,
        order_id=order_id,
    )
    return order
