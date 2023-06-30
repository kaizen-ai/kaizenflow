#!/usr/bin/env python
"""
Flatten all open positions in the account using CCXT.

Example use:

# Flatten futures live account and assert if some positions are not completely
# closed (i.e. holdings not brought to zero).
> oms/ccxt/scripts/flatten_ccxt_account.py \
    --exchange 'binance' \
    --contract_type 'futures' \
    --stage 'preprod' \
    --secret_id 4 \
    --assert_on_non_zero
"""
import argparse
import asyncio
import logging

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hprint as hprint
import im_v2.common.data.client as icdc
import im_v2.common.db.db_utils as imvcddbut
import oms.ccxt.ccxt_broker_instances as occcbrin
import oms.ccxt.ccxt_broker_v1 as occcbrv1
import oms.order as omorder

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--exchange",
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
        "--stage",
        action="store",
        required=True,
        type=str,
        help="Stage to run at: local, preprod, prod.",
    )
    parser.add_argument(
        "--secret_id",
        action="store",
        required=True,
        type=int,
        help="ID of the API Keys to use as they are stored in AWS SecretsManager.",
    )
    parser.add_argument(
        "--assert_on_non_zero_positions",
        action="store_true",
        required=False,
        help="Assert if the open positions failed to close completely.",
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _flatten_ccxt_account(
    broker: occcbrv1.CcxtBroker_v1, dry_run: bool, *, deadline_in_secs: int = 60
) -> None:
    """
    Close all open positions on the CCXT account.

    The positions are closed via reduce-only orders of the opposite
    side of the current position, with larger order amount.
    E.g. for a position of `"BTC/USDT": -0.1`, a reduce-only order of "BTC/USDT": 0.1" is placed.

    Note: currently optimized for Binance futures, removing all long/short positions

    :param broker: a CCXT broker object
    :param dry_run: whether to avoid actual execution
    :param deadline_in_secs: deadline for order to be executed, 60 by default
    """
    # Fetch all open positions.
    open_positions = broker.get_open_positions()
    if open_positions:
        # Create orders.
        orders = []
        # TODO(Danya): Replace with reduce only, maybe close all.
        for symbol, position in open_positions.items():
            _LOG.info(symbol, position)
            # Build an order to flatten the account.
            type_ = "market"
            curr_num_shares = float(position)
            diff_num_shares = -curr_num_shares
            asset_id = broker.ccxt_symbol_to_asset_id_mapping[symbol]
            curr_timestamp = pd.Timestamp.now(tz="UTC")
            start_timestamp = curr_timestamp
            end_timestamp = start_timestamp + pd.DateOffset(
                seconds=deadline_in_secs
            )
            order_id = 0
            order = omorder.Order(
                curr_timestamp,
                asset_id,
                type_,
                start_timestamp,
                end_timestamp,
                curr_num_shares,
                diff_num_shares,
                order_id=order_id,
            )
            # Set order as reduce-only to close the position.
            order.extra_params["reduce_only"] = True
            orders.append(order)
        asyncio.run(broker.submit_orders(orders, dry_run=dry_run))
    else:
        _LOG.warning("No open positions found.")
    _LOG.info("Account flattened. Total balance: %s", broker.get_total_balance())


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Initialize IM client.
    #  Data is not needed in this case, so connecting to `dev` DB.
    connection = imvcddbut.DbConnectionManager.get_connection("dev")
    im_client = icdc.get_mock_realtime_client("v1", connection)
    market_data = occcbrin.get_RealTimeImClientMarketData_example2(im_client)
    exchange = args.exchange
    contract_type = args.contract_type
    stage = args.stage
    secret_id = args.secret_id
    # Initialize broker.
    # TODO(Danya): Replace with `get_CcxtBroker_v1_prod_instance1`.
    broker = occcbrin.get_CcxtBroker_example1(
        market_data, exchange, contract_type, stage, secret_id
    )
    # Close all open positions via reduce-only orders.
    _flatten_ccxt_account(broker, dry_run=False)
    if args.assert_on_non_zero_positions:
        # Assert that all positions are closed.
        open_positions = broker.get_open_positions()
        hdbg.dassert_eq(
            len(open_positions),
            0,
            msg="Some positions failed to close: %s"
            % hprint.to_str("open_positions"),
        )


if __name__ == "__main__":
    _main(_parse())
