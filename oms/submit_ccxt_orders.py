#!/usr/bin/env python
"""
Flatten all open positions in test account using CCXT.

Example use:

# Flatten futures account in Binance testnet.
> oms/flatten_ccxt_account.py \
    --exchange_id 'binance' \
    --contract_type 'futures'
"""
import argparse
import asyncio
import logging
import time

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hsql as hsql
import im_v2.common.data.client as icdc
import im_v2.im_lib_tasks as imvimlita
import oms.oms_ccxt_utils as oomccuti
import oms.order as omorder

_LOG = logging.getLogger(__name__)


DEFAULT_ORDERS = """Order: order_id=0 creation_timestamp=2022-08-05 10:36:44.976104-04:00 asset_id=1464553467 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=0.121 tz=America/New_York
Order: order_id=1 creation_timestamp=2022-08-05 10:36:44.976104-04:00 asset_id=1467591036 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=0.011 tz=America/New_York
Order: order_id=2 creation_timestamp=2022-08-05 10:36:44.976104-04:00 asset_id=2061507978 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=169.063 tz=America/New_York
Order: order_id=3 creation_timestamp=2022-08-05 10:36:44.976104-04:00 asset_id=2237530510 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=2.828 tz=America/New_York
Order: order_id=4 creation_timestamp=2022-08-05 10:36:44.976104-04:00 asset_id=2601760471 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=-33.958 tz=America/New_York
Order: order_id=5 creation_timestamp=2022-08-05 10:36:44.976104-04:00 asset_id=3065029174 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=6052.094 tz=America/New_York
Order: order_id=6 creation_timestamp=2022-08-05 10:36:44.976104-04:00 asset_id=3303714233 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=-0.07 tz=America/New_York
Order: order_id=7 creation_timestamp=2022-08-05 10:36:44.976104-04:00 asset_id=8717633868 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=3.885 tz=America/New_York
Order: order_id=8 creation_timestamp=2022-08-05 10:36:44.976104-04:00 asset_id=8968126878 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=1.384 tz=America/New_York"""


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--exchange_id",
        action="store",
        required=True,
        type=str,
        help="Name of the exchange, e.g. 'binance'.",
    )
    parser.add_argument(
        "--contract_type",
        action="store",
        required=True,
        type=str,
        help="'futures' or 'spot'. Note: only futures contracts are supported.",
    )
    parser.add_argument(
        "--sleep_in_seconds",
        action="store",
        required=True,
        type=int,
        help="Sleep time between order submissions in seconds.",
    )
    parser.add_argument(
        "--orders_file",
        action="store",
        required=False,
        default=None,
        type=str,
        help="Orders in string format",
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _LOG.debug("Initializing broker.")
    # Get environment variables with login info.
    env_file = imvimlita.get_db_env_path("dev")
    # Get login info.
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    # Login.
    connection = hsql.get_connection(*connection_params)
    # Remove table if it already existed.
    hsql.remove_table(connection, "example2_marketdata")
    # Initialize real-time market data.
    im_client = icdc.get_mock_realtime_client(connection)
    market_data = oomccuti.get_RealTimeImClientMarketData_example2(im_client)
    # Initialize CcxtBroker connected to testnet.
    exchange_id = args.exchange_id
    contract_type = args.contract_type
    stage = "preprod"
    secret_id = 1
    broker = oomccuti.get_CcxtBroker_example1(
        market_data, exchange_id, contract_type, stage, secret_id
    )
    if args.orders_file is None:
        orders = omorder.orders_from_string(DEFAULT_ORDERS)
    else:
        hdbg.dassert_file_exists(args.orders_file)
        with open(args.orders_file, "r") as f:
            orders_as_txt = f.read()
            _LOG.info("Orders as string: %s", orders_as_txt)
            orders = omorder.orders_from_string(orders_as_txt)
    _LOG.info("All orders: %s", [str(order) for order in orders])
    for order in orders:
        # Update order type to one supported by CCXT.
        order.type_ = "market"
        _LOG.info("Submitting order: %s", str(order))
        asyncio.run(broker.submit_orders([order]))
        _LOG.info("Orders submitted, sleeping for %s secs", args.sleep_in_seconds)
        time.sleep(args.sleep_in_seconds)
        _LOG.info("Getting fills...")
        fills = broker.get_fills()
        _LOG.info("Fills: %s", [str(fill) for fill in fills])


if __name__ == "__main__":
    _main(_parse())
