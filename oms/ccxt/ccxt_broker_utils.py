"""
Hi-level functions for CCXT broker.

Import as:

import oms.ccxt.ccxt_broker_utils as occcbrut
"""
import asyncio
import logging
import os
from typing import Dict, Optional

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hprint as hprint
import im_v2.common.data.client as icdc
import im_v2.common.db.db_utils as imvcddbut
import oms.ccxt.abstract_ccxt_broker as ocabccbr
import oms.ccxt.ccxt_broker_instances as occcbrin
import oms.order as omorder

_LOG = logging.getLogger(__name__)


def get_broker(
    exchange: str,
    contract_type: str,
    stage: str,
    secret_id: int,
    *,
    db_stage: Optional[str] = None,
) -> ocabccbr.AbstractCcxtBroker:
    """
    Initialize and get broker.

    :param exchange: exchange name, e.g., "binance"
    :param contract_type: contract type, e.g., "futures"
    :param stage: stage, e.g., "dev"
    :param secret_id: secret id of the broker
    :param db_stage: stage of the database to use, e.g., "dev"
    :return: broker
    """
    if db_stage is None:
        db_stage = "dev"
    # TODO(Vlad): Remove dependency on having a functional DB connection,
    # it makes no sense here.
    connection = imvcddbut.DbConnectionManager.get_connection(db_stage)
    im_client = icdc.get_mock_realtime_client("v1", connection)
    market_data = occcbrin.get_RealTimeImClientMarketData_example2(im_client)
    broker = occcbrin.get_CcxtBroker_example1(
        market_data, exchange, contract_type, stage, secret_id
    )
    return broker


def save_to_json_file_and_log(
    data: Dict[str, float],
    log_dir: str,
    enclose_dir: str,
    exchange: str,
    contract_type: str,
    file_type: str,
) -> None:
    """
    Save data to a json file and log the file path.

    :param data: data to save
    :param log_dir: directory to save the file to
    :param enclose_dir: directory to enclose the file in
    :param exchange: exchange name, e.g., "binance"
    :param contract_type: contract type, e.g., "futures"
    :param file_type: file type, e.g., "balance" or "open_positions"
    """
    hio.create_dir(log_dir, incremental=True, backup_dir_if_exists=True)
    # Get enclosed balance directory.
    enclosing_dir = os.path.join(log_dir, enclose_dir)
    hio.create_dir(enclosing_dir, incremental=True)
    # Get file name, e.g.
    #  '/shared_data/system_log_dir/total_balance/'
    #  'binance_futures_balance_20230419_172022.json'
    timestamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%d_%H%M%S")
    file_name = f"{exchange}_{contract_type}_{file_type}_{timestamp}.json"
    file_path = os.path.join(enclosing_dir, file_name)
    # Save.
    hio.to_json(file_path, data)
    _LOG.debug("%s saved to %s", enclose_dir, file_path)
    _LOG.info(hprint.to_pretty_str(data))


def get_ccxt_total_balance(
    broker: ocabccbr.AbstractCcxtBroker,
    log_dir: str,
    exchange: str,
    contract_type: str,
) -> None:
    """
    Get total balance of the account.

    :param broker: broker object
    :param log_dir: directory to save the balance to
    :param exchange: exchange name, e.g. 'binance'
    :param contract_type: 'futures' or 'spot'
    """
    # Get total balance.
    total_balance = broker.get_total_balance()
    enclose_dir = "total_balance"
    file_type = "balance"
    save_to_json_file_and_log(
        total_balance, log_dir, enclose_dir, exchange, contract_type, file_type
    )


def get_ccxt_open_positions(
    broker: ocabccbr.AbstractCcxtBroker,
    log_dir: str,
    exchange: str,
    contract_type: str,
) -> None:
    open_positions = broker.get_open_positions()
    enclose_dir = "open_positions"
    file_type = "open_positions"
    save_to_json_file_and_log(
        open_positions, log_dir, enclose_dir, exchange, contract_type, file_type
    )


def flatten_ccxt_account(
    broker: ocabccbr.AbstractCcxtBroker,
    dry_run: bool,
    *,
    deadline_in_secs: int = 60,
    assert_on_non_zero_positions: bool = False,
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
            _LOG.info("%s - %s", symbol, position)
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
    if assert_on_non_zero_positions:
        # Assert that all positions are closed.
        open_positions = broker.get_open_positions()
        hdbg.dassert_eq(
            len(open_positions),
            0,
            msg=(
                "Some positions failed to close: "
                f"{hprint.to_str('open_positions')}"
            ),
        )


def describe_and_flatten_account(
    broker: ocabccbr.AbstractCcxtBroker,
    exchange: str,
    contract_type: str,
    log_dir: str,
    *,
    assert_on_non_zero_positions: bool = False,
) -> None:
    """
    Describe and flatten the account.

    :param exchange: exchange name, e.g. 'binance'
    :param contract_type: 'futures' or 'spot'
    :param log_dir: directory to save the balance to
    :param assert_on_non_zero_positions: whether to assert on non-zero positions
    """
    _LOG.info("Getting balances...")
    get_ccxt_total_balance(broker, log_dir, exchange, contract_type)
    _LOG.info("Getting open positions...")
    get_ccxt_open_positions(broker, log_dir, exchange, contract_type)
    _LOG.info("Flattening account...")
    flatten_ccxt_account(
        broker,
        dry_run=False,
        assert_on_non_zero_positions=assert_on_non_zero_positions,
    )
    _LOG.info("Getting balances...")
    get_ccxt_total_balance(broker, log_dir, exchange, contract_type)
    _LOG.info("Getting open positions...")
    get_ccxt_open_positions(broker, log_dir, exchange, contract_type)
