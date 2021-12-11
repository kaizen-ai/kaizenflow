"""
Import as:

import oms.order_example as oordexam
"""

import logging

import pandas as pd

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
