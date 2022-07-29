"""
Import as:

import oms.oms_utils as oomsutil
"""
import collections
import logging
from typing import Dict, List

import async
import numpy as np
import pandas as pd

import helpers.hdbg as hdbg
import oms.ccxt_broker as occxbrok
import oms.order as omorder

_LOG = logging.getLogger(__name__)


def _timestamp_to_str(timestamp: pd.Timestamp) -> str:
    """
    Print timestamp as string only in terms of time.

    This is useful to simplify the debug output for intraday trading.
    """
    val = "'%s'" % str(timestamp.time())
    return val


def _get_col_name(col_name: str, prefix: str) -> str:
    if prefix != "":
        col_name = prefix + "." + col_name
    return col_name


# #############################################################################
# Accounting functions.
# #############################################################################

# Represent a set of DataFrame columns that is built incrementally.
Accounting = Dict[str, List[float]]


def _create_accounting_stats(columns: List[str]) -> Accounting:
    """
    Create incrementally built dataframe with the given columns.
    """
    accounting = collections.OrderedDict()
    for column in columns:
        accounting[column] = []
    return accounting


def _append_accounting_df(
    df: pd.DataFrame,
    accounting: Accounting,
    prefix: str,
) -> pd.DataFrame:
    """
    Update `df` with the intermediate results stored in `accounting`.
    """
    dfs = []
    for key, value in accounting.items():
        _LOG.debug("key=%s", key)
        # Pad the data so that it has the same length as `df`.
        num_vals = len(accounting[key])
        num_pad = df.shape[0] - num_vals
        hdbg.dassert_lte(0, num_pad)
        buffer = [np.nan] * num_pad
        # Get the target column name.
        col_name = _get_col_name(key, prefix)
        # Create the column of the data frame.
        df_out = pd.DataFrame(value + buffer, index=df.index, columns=[col_name])
        hdbg.dassert_eq(df_out.shape[0], df.shape[0])
        dfs.append(df_out)
    # Concat all the data together with the input.
    df_out = pd.concat([df] + dfs, axis=1)
    return df_out


def flatten_ccxt_account(
    broker: occxbrok.CcxtBroker,
    dry_run: bool,
):
    """
    Remove all crypto assets/positions from the test accound.

    Note: currently optimized for futures, removing all long/short positions

    :param broker: a CCXT broker object
    :param dry_run: whether to avoid actual execution
    :return:
    """
    # Verify that the broker is in test mode.
    hdbg.dassert_eq(
        broker._mode,
        "test",
        msg="Account flattening is supported only for test accounts.",
    )
    # Fetch all open positions.
    positions = broker._exchange.fetchPositions()
    open_positions = []
    for position in positions:
        # Get the quantity of assets on short/long positions.
        position_amount = float(position["info"]["positionAmt"])
        if position_amount != 0:
            open_positions.append(position)
    if open_positions:
        # Create orders.
        orders = []
        for position in open_positions:
            # Build an order to flatten the account.
            type_ = "market"
            curr_num_shares = float(position["info"]["positionAmt"])
            diff_num_shares = -curr_num_shares
            full_symbol = position["symbol"].replace("/", "_")
            asset_id = broker._symbol_to_asset_id_mapping[full_symbol]
            curr_timestamp = pd.Timestamp.now(tz="UTC")
            start_timestamp = curr_timestamp
            end_timestamp = start_timestamp + pd.DateOffset(minutes=1)
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
            orders.append(order)
        broker.submit_orders(orders, dry_run=dry_run)
    else:
        _LOG.warning("No open positions found.")
    _LOG.info("Account flattened. Total balance: %s", broker.get_total_balance())
