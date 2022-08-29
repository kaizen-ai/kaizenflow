"""
Import as:

import oms.order_processor_example as oorprexa
"""

import logging
from typing import Coroutine

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hsql as hsql
import oms.order_processor as oordproc
import oms.portfolio as omportfo

_LOG = logging.getLogger(__name__)


# TODO(gp): -> `get_OrderProcessor_example()`.
def get_order_processor_example1(
    db_connection: hsql.DbConnection,
    portfolio: omportfo.Portfolio,
    asset_id_name: str,
    max_wait_time_for_order_in_secs: int,
    *,
    delay_to_accept_in_secs: int = 3,
    delay_to_fill_in_secs: int = 10
) -> oordproc.OrderProcessor:
    """
    Build an OrderProcessor.

    The params are the same as the OrderProcessor.

    :param max_wait_time_for_order_in_secs: how long to wait for an order to be
        received, once this object starts waiting
    :param delay_to_accept_in_secs: delay after the order is submitted to update
        the accepted orders table
    :param delay_to_fill_in_secs: delay after the order is accepted to update the
        position table with the filled positions
    """
    broker = portfolio.broker
    order_processor = oordproc.OrderProcessor(
        db_connection,
        max_wait_time_for_order_in_secs,
        delay_to_accept_in_secs,
        delay_to_fill_in_secs,
        broker,
        asset_id_name,
    )
    return order_processor


# TODO(gp): It would be better to pass only what's needed (i.e., get_wall_clock_time)
#  instead of passing Portfolio.
# TODO(gp): -> `get_OrderProcessorCoroutine_example()`.
def get_order_processor_coroutine_example1(
    order_processor: oordproc.OrderProcessor,
    portfolio: omportfo.Portfolio,
    duration_in_secs: float,
) -> Coroutine:
    """
    Create a coroutine running the OrderProcessor, that lasts for
    duration_in_secs.

    :param duration_in_secs: how many seconds to run after the beginning of the
        replayed clock
    """
    _LOG.debug(hprint.to_str("order_processor portfolio duration_in_secs"))
    # Compute the timestamp when the OrderProcessor should shut down.
    get_wall_clock_time = portfolio.broker.market_data.get_wall_clock_time
    initial_replayed_timestamp = get_wall_clock_time()
    hdbg.dassert_isinstance(duration_in_secs, (float, int))
    hdbg.dassert_lt(0, duration_in_secs)
    offset = pd.Timedelta(duration_in_secs, unit="seconds")
    termination_timestamp = initial_replayed_timestamp + offset
    # Build the Coroutine.
    order_processor_coroutine = order_processor.run_loop(termination_timestamp)
    hdbg.dassert_isinstance(order_processor_coroutine, Coroutine)
    return order_processor_coroutine