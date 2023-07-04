#!/usr/bin/env python

# TODO(gp): This script should only use Broker abstract interface.

"""
Run the order submission via CcxtBroker_v2 in an async manner.

Example use:

> oms/ccxt/scripts/run_ccxt_broker_v2.py \
    --log_dir /shared_data/CMTask4347_log_dir/ \
    -v DEBUG
"""
import argparse
import asyncio
import logging
import os
from typing import List

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hparser as hparser
import helpers.hprint as hprint
import oms.ccxt.ccxt_broker_instances as occcbrin
import oms.ccxt.ccxt_broker_utils as occcbrut
import oms.order as omorder

_LOG = logging.getLogger(__name__)


def get_orders() -> List[omorder.Order]:
    """
    Generate test orders for concurrent submission.

    The test orders are meant to be submitted simultaneously, and
    therefore should have different `asset_ids` to avoid collision.
    """
    # pylint: disable=line-too-long
    orders_str = "\n".join(
        [
            # DOGE
            "Order: order_id=0 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=3065029174 type_=price@twap start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=-900.0 tz=America/New_York extra_params={}",
            # APE
            "Order: order_id=1 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=6051632686 type_=limit start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=33.0 tz=America/New_York extra_params={}",
        ]
    )
    #            "Order: order_id=2 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=8717633868 type_=limit start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=3.0 tz=America/New_York extra_params={}",
    #            "Order: order_id=3 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=8968126878 type_=limit start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=-0.69 tz=America/New_York extra_params={}",
    #            "Order: order_id=4 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=1467591036 type_=limit start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=0.005 tz=America/New_York extra_params={}",
    #            "Order: order_id=5 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=2476706208 type_=limit start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=-30.0 tz=America/New_York extra_params={}",
    #            "Order: order_id=6 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=5115052901 type_=limit start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=150.0 tz=America/New_York extra_params={}",
    #            "Order: order_id=7 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=3401245610 type_=limit start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=-9.0 tz=America/New_York extra_params={}",
    #            "Order: order_id=8 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=1464553467 type_=limit start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=0.03 tz=America/New_York extra_params={}",
    #            "Order: order_id=9 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=1966583502 type_=limit start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=200.0 tz=America/New_York extra_params={}",
    #            "Order: order_id=10 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=1030828978 type_=limit start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=100.0 tz=America/New_York extra_params={}",
    #            "Order: order_id=11 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=2683705052 type_=limit start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=-120.0 tz=America/New_York extra_params={}",
    #            "Order: order_id=12 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=9872743573 type_=limit start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=-18.0 tz=America/New_York extra_params={}",
    #            "Order: order_id=13 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=2484635488 type_=limit start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=-800.0 tz=America/New_York extra_params={}",
    #            "Order: order_id=14 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=2099673105 type_=limit start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=60.0 tz=America/New_York extra_params={}",
    #            "Order: order_id=15 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=2237530510 type_=limit start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=-5.0 tz=America/New_York extra_params={}",
    #            "Order: order_id=16 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=2425308589 type_=limit start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=130.0 tz=America/New_York extra_params={}",
    #            "Order: order_id=17 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=1776791608 type_=limit start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=13.0 tz=America/New_York extra_params={}",
    #            "Order: order_id=18 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=2384892553 type_=limit start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=-30.0 tz=America/New_York extra_params={}",
    #            "Order: order_id=19 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=5118394986 type_=limit start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=-30.0 tz=America/New_York extra_params={}",
    #        ]
    #    )
    # pylint: enable=line-too-long
    orders = omorder.orders_from_string(orders_str)
    for order in orders:
        order.creation_timestamp = hdateti.get_current_time("ET")
        _LOG.debug(hprint.to_str("order.creation_timestamp"))
        order.start_timestamp = order.creation_timestamp
        order.end_timestamp = order.start_timestamp + pd.Timedelta("5T")
    return orders


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--log_dir",
        action="store",
        required=True,
        type=str,
        help="Order logs directory.",
    )
    parser.add_argument(
        "--clean_up_before_run",
        action="store_true",
        required=False,
        help="Clean up before running the script.",
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _generate_log_dir(log_dir: str) -> str:
    """
    Generate experiment log_dir if the provided log dir already exists.
    """
    if os.path.exists(log_dir):
        _LOG.debug("log_dir=%s already exists.", log_dir)
        log_dir = f"{log_dir}_{hdateti.get_current_timestamp_as_string(tz='ET')}"
        _LOG.debug(hprint.to_str("log_dir"))
    return log_dir


def _main(parser: argparse.ArgumentParser) -> None:
    # Initialize time log for in-script operations.
    args = parser.parse_args()
    log_dir = args.log_dir
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Initialize broker for the trading account.
    secret_id = 3
    # secret_id = 4
    broker = occcbrin.get_CcxtBroker_v2(secret_id, log_dir)
    # Clean up before running the script.
    if args.clean_up_before_run:
        _LOG.debug("Cleaning up before running the script.")
        # TODO(Vlad): Temporarily hardcoded parameters.
        exchange = "binance"
        contract_type = "futures"
        occcbrut.describe_and_flatten_account(
            broker, exchange, contract_type, log_dir
        )
    # Create a directory non-incrementally to avoid overlapping experiments.
    log_dir = _generate_log_dir(log_dir)
    hio.create_dir(log_dir, incremental=False, abort_if_exists=True)
    orders = get_orders()
    _LOG.debug(str(orders))
    hdbg.dassert_no_duplicates([order.asset_id for order in orders])
    passivity_factor = 0.55
    _ = asyncio.run(broker.submit_twap_orders(orders, passivity_factor))


if __name__ == "__main__":
    _main(_parse())
