"""
Import as:

import oms.broker_example as obroexam
"""

import asyncio

import market_data.market_data_interface as mdmadain
import oms.broker as ombroker


def get_broker_example1(event_loop: asyncio.AbstractEventLoop) -> ombroker.Broker:
    # Build the price interface.
    (
        market_data_interface,
        get_wall_clock_time,
    ) = mdmadain.get_replayed_time_market_data_interface_example2(event_loop)
    # Build the broker.
    broker = ombroker.Broker(market_data_interface, get_wall_clock_time)
    # TODO(gp): Instead of returning all the objects we could return only `broker`
    #  and allow clients to extract the objects from inside, if needed.
    return broker
