#!/usr/bin/env python

# TODO(gp): This script should only use Broker abstract interface.

"""
Run the order submission via CcxtBroker_v2 in an async manner.

Example use:

> oms/broker/ccxt/scripts/run_ccxt_broker_v2.py \
    --log_dir tmp_log_dir \
    --max_order_notional 60 \
    --randomize_orders \
    --parent_order_duration_in_min 5 \
    --num_bars 3 \
    --num_parent_orders_per_bar 5 \
    --secret_id 4 \
    --clean_up_before_run \
    --clean_up_after_run \
    --close_positions_using_twap \
    --passivity_factor 0.55 \
    -v DEBUG
"""
import argparse
import asyncio
import logging
import os
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

import helpers.hasyncio as hasynci
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hparser as hparser
import helpers.hprint as hprint
import helpers.hwall_clock_time as hwacltim
import im_v2.common.universe as ivcu
import oms.broker.ccxt.abstract_ccxt_broker as obcaccbr
import oms.broker.ccxt.ccxt_broker_instances as obccbrin
import oms.broker.ccxt.ccxt_broker_utils as obccbrut
import oms.broker.ccxt.ccxt_broker_v2 as obccbrv2
import oms.order.order as oordorde

_LOG = logging.getLogger(__name__)

_BINANCE_MIN_NOTIONAL = 10
_UNIVERSE_VERSION = "v7.1"
_VENDOR = "ccxt"
_EXCHANGE = "binance"
# TODO(Juraj): Implement fetching prices from our DB.
_HARDCODED_PRICES = {
    "BTC/USDT:USDT": 29280.6,
    "ETH/USDT:USDT": 1870.57,
    "XRP/USDT:USDT": 0.7113,
    "APE/USDT:USDT": 1.964,
    "AVAX/USDT:USDT": 13.181,
    "AXS/USDT:USDT": 6.113,
    "BAKE/USDT:USDT": 0.0937,
    "BNB/USDT:USDT": 241.46,
    "CRV/USDT:USDT": 0.728,
    "CTK/USDT:USDT": 0.5871,
    "DOGE/USDT:USDT": 0.07752,
    "DOT/USDT:USDT": 5.239,
    "DYDX/USDT:USDT": 2.068,
    "FTM/USDT:USDT": 0.245,
    "GMT/USDT:USDT": 0.2141,
    "LINK/USDT:USDT": 7.751,
    "MATIC/USDT:USDT": 0.7105,
    "NEAR/USDT:USDT": 1.394,
    "OGN/USDT:USDT": 0.112,
    "RUNE/USDT:USDT": 0.937,
    "SAND/USDT:USDT": 0.43,
    "SOL/USDT:USDT": 25.069,
    "STORJ/USDT:USDT": 0.309,
    "UNFI/USDT:USDT": 3.259,
    "WAVES/USDT:USDT": 1.926,
}
# TODO(Juraj): a hacky way to make progress, implement this as one of the run modes.
_NEXT_ORDER_SIDE_PER_ASSET = {
    "BTC/USDT:USDT": None,
    "ETH/USDT:USDT": None,
    "XRP/USDT:USDT": None,
    "APE/USDT:USDT": None,
    "AVAX/USDT:USDT": None,
    "AXS/USDT:USDT": None,
    "BAKE/USDT:USDT": None,
    "BNB/USDT:USDT": None,
    "CRV/USDT:USDT": None,
    "CTK/USDT:USDT": None,
    "DOGE/USDT:USDT": None,
    "DOT/USDT:USDT": None,
    "DYDX/USDT:USDT": None,
    "FTM/USDT:USDT": None,
    "GMT/USDT:USDT": None,
    "LINK/USDT:USDT": None,
    "MATIC/USDT:USDT": None,
    "NEAR/USDT:USDT": None,
    "OGN/USDT:USDT": None,
    "RUNE/USDT:USDT": None,
    "SAND/USDT:USDT": None,
    "SOL/USDT:USDT": None,
    "STORJ/USDT:USDT": None,
    "UNFI/USDT:USDT": None,
    "WAVES/USDT:USDT": None,
}


def _get_symbols(
    universe_version: str,
    vendor: str,
    exchange: str,
) -> List[str]:
    """
    Get top symbols from the universe.

    :param universe_version: version of the universe
    :param vendor: vendor name
    :param exchange: exchange name
    :return: list of CCXT formatted symbols
    """
    # Get the universe.
    mode = "trade"
    universe = ivcu.get_vendor_universe(vendor, mode, version=universe_version)
    # Reformat symbols.
    symbols = [obccbrv2.CcxtBroker_v2._convert_currency_pair_to_ccxt_format(symbol) for symbol in universe[exchange]]
    return symbols


def _get_asset_ids(
    broker: obcaccbr.AbstractCcxtBroker, *, number_asset_ids: Optional[int] = None
) -> List[dict]:
    """
    Get random asset ids.

    :param broker: broker instance
    :param number_asset_ids: number of asset ids to return
    :return: list of asset ids
    """
    _LOG.debug("number_asset_ids=%s", number_asset_ids)
    if number_asset_ids is None:
        number_asset_ids = np.random.randint(5, 10)
    top_symbols = _get_symbols(_UNIVERSE_VERSION, _VENDOR, _EXCHANGE)
    # Get asset ids.
    asset_ids = [
        {
            "asset_id": broker.ccxt_symbol_to_asset_id_mapping[symbol],
            "price": _HARDCODED_PRICES[symbol],
            "symbol": symbol,
        }
        for symbol in top_symbols
    ]
    _LOG.debug("prices_by_asset_ids=%s", asset_ids)
    asset_ids = asset_ids[:number_asset_ids]
    _LOG.debug("asset_ids=%s", asset_ids)
    return asset_ids


def _get_random_order(
    start_timestamp: pd.Timestamp,
    asset_id: int,
    symbol: str,
    price: float,
    max_order_notional: float,
    parent_order_duration_in_min: int,
    current_position: float,
    close_position: bool,
    *,
    order_direction: Optional[str] = None,
) -> oordorde.Order:
    """
    Get random order.

    :param asset_id: asset id
    :param price: price
    :param max_order_notional: max order notional
    :return: order
    """
    # Order type.
    order_type = "price@twap"
    # Order size.
    min_order_size = _BINANCE_MIN_NOTIONAL / price * parent_order_duration_in_min
    max_order_size = max_order_notional / price * parent_order_duration_in_min
    if close_position:
        num_shares = current_position * -1
    else:
        num_shares = np.random.uniform(min_order_size, max_order_size)
        _NEXT_ORDER_SIDE_PER_ASSET[symbol] = "sell"
        # Set negative amount of shares when direction is sell, or direction should be random.
        if order_direction == "sell" or (
            order_direction is None and np.random.choice([True, False])
        ):
            num_shares = -num_shares
            _NEXT_ORDER_SIDE_PER_ASSET[symbol] = "buy"
    _LOG.debug(hprint.to_str2(num_shares))
    # Timestamps.
    end_timestamp = start_timestamp + pd.Timedelta(
        minutes=parent_order_duration_in_min
    )
    diff_num_shares = float(num_shares)
    _LOG.debug(hprint.to_str2(diff_num_shares))
    if diff_num_shares == 0:
        return None
    # Create the order.
    order = oordorde.Order(
        start_timestamp,
        asset_id,
        order_type,
        start_timestamp,
        end_timestamp,
        current_position,
        diff_num_shares,
    )
    _LOG.debug("order=%s", order)
    return order


def _get_random_orders(
    broker: obcaccbr.AbstractCcxtBroker,
    max_order_notional: int,
    parent_order_duration_in_min: int,
    number_orders: int,
    positions: Dict[int, float],
    start_timestamp: pd.Timestamp,
    *,
    close_positions: bool = False,
    orders_direction: Optional[str] = None,
) -> List[oordorde.Order]:
    """
    Get random orders.

    :param broker: broker instance
    :param max_order_notional: max order notional
    :param parent_order_duration_in_min: run length in minutes
    :param number_orders: number of orders to return
    :param orders_directions: "buy", "sell", if None then randomly chosen.
    :return: list of orders
    """
    asset_ids = _get_asset_ids(broker, number_asset_ids=number_orders)
    orders = []
    for asset_id in asset_ids:
        position = positions.get(asset_id["asset_id"], 0)
        if close_positions and position == 0:
            continue
        # Set order direction for the current order.
        symbol = asset_id["symbol"]
        order_diretion = (
            orders_direction
            if orders_direction is not None
            else _NEXT_ORDER_SIDE_PER_ASSET[symbol]
        )
        order = _get_random_order(
            start_timestamp,
            asset_id["asset_id"],
            symbol,
            asset_id["price"],
            max_order_notional,
            parent_order_duration_in_min,
            position,
            close_positions,
            order_direction=order_diretion,
        )
        if order is not None:
            orders.append(order)
    _LOG.debug("random_orders=%s", str(orders))
    return orders


def get_orders() -> List[oordorde.Order]:
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
    orders = oordorde.orders_from_string(orders_str)
    start_timestamp = hdateti.get_current_time("ET")
    end_timestamp = start_timestamp + pd.Timedelta("5T")
    for order in orders:
        order.creation_timestamp = hdateti.get_current_time("ET")
        _LOG.debug(hprint.to_str("order.creation_timestamp"))
        order.start_timestamp = start_timestamp
        order.end_timestamp = end_timestamp
    return orders


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--secret_id",
        required=False,
        default=4,
        type=int,
        help="Secret ID.",
    )
    parser.add_argument(
        "--max_order_notional",
        required=False,
        default=25,
        type=int,
        help="Maximal order notional. Must be atleast 9 * parent_order_duration_in_min",
    )
    parser.add_argument(
        "--randomize_orders",
        action="store_true",
        required=False,
        default=False,
        help="Randomize orders.",
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
        help="Clean up before submitting orders.",
    )
    parser.add_argument(
        "--clean_up_after_run",
        action="store_true",
        required=False,
        help="Clean up after submitting orders.",
    )
    parser.add_argument(
        "--parent_order_duration_in_min",
        required=False,
        default=3,
        type=int,
        help="Length of the run in minutes.",
    )
    parser.add_argument(
        "--num_parent_orders_per_bar",
        required=False,
        default=10,
        type=int,
        help="Number of orders (using unique symbols) to generate per bar",
    )
    parser.add_argument(
        "--num_bars",
        required=False,
        default=1,
        type=int,
        help="Number of bars to execute (assuming parent_order_duration_in_min = 5)",
    )
    parser.add_argument(
        "--close_positions_using_twap",
        action="store_true",
        required=False,
        default=False,
        help="Additional bar is added which submits order to flatten positions (caution: fills are not guaranteed)",
    )
    parser.add_argument(
        "--passivity_factor",
        required=True,
        type=float,
        help="Passivity factor used during calculation of TWAP price",
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


def _execute_one_bar_using_twap(
    args: Dict,
    broker: obccbrv2.CcxtBroker_v2,
    passivity_factor: float,
    *,
    close_positions: bool = False,
    previous_start_timestamp: Optional[pd.Timestamp] = None,
    orders_direction: Optional[str] = None,
) -> pd.Timestamp:
    """
    Generate random orders and submit them in a TWAP manner.

    :return: start timestamp of the parent orders
    """
    # # Ceil the timestamp to the nearest minute so end_timestamp can be calculated correctly.
    # start_timestamp = pd.Timestamp.utcnow().ceil('T')
    # Align start_timestamp to a 5 min grid.
    # TODO(Danya): CMTask4860.
    if previous_start_timestamp is None:
        start_timestamp = pd.Timestamp.utcnow().ceil("5T")
    else:
        start_timestamp = previous_start_timestamp + pd.Timedelta(minutes=5)
    if args.randomize_orders:
        positions = broker.get_open_positions()
        positions = {
            broker.ccxt_symbol_to_asset_id_mapping[key]: value
            for key, value in positions.items()
        }
        orders = _get_random_orders(
            broker,
            args.max_order_notional,
            args.parent_order_duration_in_min,
            number_orders=args.num_parent_orders_per_bar,
            positions=positions,
            start_timestamp=start_timestamp,
            close_positions=close_positions,
            orders_direction=orders_direction,
        )
        _LOG.debug(str(orders))
    else:
        orders = get_orders()
    _LOG.debug(str(orders))
    hdbg.dassert_no_duplicates([order.asset_id for order in orders])
    # TODO(Danya): A hack to allow correct logging of multiple parent orders
    hwacltim.set_current_bar_timestamp(orders[0].creation_timestamp)
    hasynci.run(
        broker.submit_twap_orders(orders, passivity_factor),
        asyncio.get_event_loop(),
        close_event_loop=False,
    )
    return start_timestamp


def _main(parser: argparse.ArgumentParser) -> None:
    # Initialize time log for in-script operations.
    args = parser.parse_args()
    passivity_factor = args.passivity_factor
    log_dir = args.log_dir
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Initialize broker for the trading account.
    secret_id = args.secret_id
    exchange = "binance"
    contract_type = "futures"
    broker = obccbrin.get_CcxtBroker_v2(secret_id, log_dir)
    # Clean up before running the script.
    if args.clean_up_before_run:
        _LOG.debug("Cleaning up before running the script.")
        # TODO(Vlad): Temporarily hardcoded parameters.
        obccbrut.describe_and_flatten_account(
            broker, exchange, contract_type, log_dir
        )
    # Create a directory non-incrementally to avoid overlapping experiments.
    log_dir = _generate_log_dir(log_dir)
    hio.create_dir(log_dir, incremental=False, abort_if_exists=True)
    try:
        previous_start_timestamp = None
        # orders_direction = "buy"
        for _ in range(args.num_bars):
            previous_start_timestamp = _execute_one_bar_using_twap(
                args,
                broker,
                passivity_factor,
                previous_start_timestamp=previous_start_timestamp,
                # orders_direction=orders_direction
            )
            # Switch between buy and sell orders in order to avoid position build-up.
            # orders_direction = "sell" if orders_direction == "buy" else "buy"
        if args.close_positions_using_twap:
            _execute_one_bar_using_twap(
                args,
                broker,
                passivity_factor,
                previous_start_timestamp=previous_start_timestamp,
                close_positions=True,
            )
    except Exception as e:
        raise e
    # Run the order submission but flatten in case some positions
    #  remained open.
    finally:
        # Clean up after running the script.
        if args.clean_up_after_run:
            _LOG.debug("Cleaning up after running the script.")
            obccbrut.describe_and_flatten_account(
                broker, exchange, contract_type, log_dir
            )


if __name__ == "__main__":
    seed = int(pd.Timestamp.now().timestamp())
    np.random.seed(seed)
    _LOG.debug("seed=%s", seed)
    _main(_parse())
