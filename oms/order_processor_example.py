"""
Import as:

import oms.order_processor_example as oorprexa
"""

import logging
from typing import Coroutine

import pandas as pd

import helpers.hasyncio as hasynci
import helpers.hsql as hsql
import oms.order_processor as oordproc
import oms.portfolio as omportfo

_LOG = logging.getLogger(__name__)


# TODO(gp): Part of this should become a `get_OrderProcessor_example()`.
def get_order_processor_example1(
    db_connection: hsql.DbConnection,
    portfolio: omportfo.Portfolio,
) -> oordproc.OrderProcessor:
    """
    Build an order processor.
    """
    # order_processor_poll_kwargs["sleep_in_secs"] = 1
    # Since orders should come every 5 mins we give it a buffer of 15 extra
    # mins.
    # TODO(gp): Expose this through the interface.
    delay_to_accept_in_secs = 3
    delay_to_fill_in_secs = 10
    broker = portfolio.broker
    asset_id_name = "asset_id"
    order_processor = oordproc.OrderProcessor(
        db_connection,
        delay_to_accept_in_secs,
        delay_to_fill_in_secs,
        broker,
        asset_id_name
        # poll_kwargs=order_processor_poll_kwargs,
    )
    return order_processor


def get_order_processor_coroutine_example1(
    order_processor: oordproc.OrderProcessor,
    portfolio: omportfo.Portfolio,
    real_time_loop_time_out_in_secs: int,
) -> Coroutine:
    # TODO(gp): It would be better to pass only what's needed (i.e.,
    #  get_wall_clock_time) instead of passing portfolio.
    get_wall_clock_time = portfolio.broker.market_data.get_wall_clock_time
    initial_timestamp = get_wall_clock_time()
    offset = pd.Timedelta(real_time_loop_time_out_in_secs, unit="seconds")
    termination_condition = initial_timestamp + offset
    order_processor_coroutine = order_processor.run_loop(termination_condition)
    return order_processor_coroutine
