"""
Import as:

import oms.order_example as oordexam
"""

import logging

import pandas as pd

import helpers.hdbg as hdbg
import oms.order as omorder

_LOG = logging.getLogger(__name__)


def get_order_example1() -> omorder.Order:
    creation_timestamp = pd.Timestamp(
        "2000-01-01 09:30:00-05:00", tz="America/New_York"
    )
    asset_id = 101
    type_ = "price@twap"
    start_timestamp = pd.Timestamp(
        "2000-01-01 09:35:00-05:00", tz="America/New_York"
    )
    end_timestamp = pd.Timestamp(
        "2000-01-01 09:40:00-05:00", tz="America/New_York"
    )
    curr_num_shares = 0
    diff_num_shares = 100
    order_id = 0
    # Build Order.
    order = omorder.Order(
        creation_timestamp,
        asset_id,
        type_,
        start_timestamp,
        end_timestamp,
        curr_num_shares,
        diff_num_shares,
        order_id=order_id,
    )
    return order


def get_order_example2() -> omorder.Order:
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
    curr_num_shares = 0
    diff_num_shares = 100
    order_id = 0
    # Build Order.
    order = omorder.Order(
        creation_timestamp,
        asset_id,
        type_,
        start_timestamp,
        end_timestamp,
        curr_num_shares,
        diff_num_shares,
        order_id=order_id,
    )
    return order


def get_order_example3(
    percent_paid: float = 0.3,
    diff_num_shares: float = 100,
) -> omorder.Order:
    hdbg.dassert_lte(0.0, percent_paid)
    hdbg.dassert_lte(percent_paid, 1.0)
    creation_timestamp = pd.Timestamp(
        "2000-01-01 09:29:00-05:00", tz="America/New_York"
    )
    asset_id = 101
    percent_paid_as_str = str(percent_paid)
    type_ = "partial_spread_" + percent_paid_as_str + "@twap"
    start_timestamp = pd.Timestamp(
        "2000-01-01 09:30:00-05:00", tz="America/New_York"
    )
    end_timestamp = pd.Timestamp(
        "2000-01-01 09:35:00-05:00", tz="America/New_York"
    )
    curr_num_shares = 0
    order_id = 0
    # Build Order.
    order = omorder.Order(
        creation_timestamp,
        asset_id,
        type_,
        start_timestamp,
        end_timestamp,
        curr_num_shares,
        diff_num_shares,
        order_id=order_id,
    )
    return order
