"""
Import as:

import oms.oms_ccxt_utils as oomccuti
"""
import asyncio
import logging

import pandas as pd

import helpers.hdbg as hdbg
import im_v2.common.data.client as icdc
import market_data as mdata
import oms.ccxt_broker as occxbrok
import oms.hsecrets.secret_identifier as ohsseide
import oms.order as omorder

_LOG = logging.getLogger(__name__)


# #############################################################################
# CCXT Broker Utilities
# #############################################################################


def flatten_ccxt_account(
    broker: occxbrok.CcxtBroker, dry_run: bool, *, deadline_in_secs: int = 60
) -> None:
    """
    Remove all crypto assets/positions from the test accound.

    Note: currently optimized for futures, removing all long/short positions

    :param broker: a CCXT broker object
    :param dry_run: whether to avoid actual execution
    :param deadline_in_secs: deadline for order to be executed, 60 by default
    """
    # Fetch all open positions.
    open_positions = broker.get_open_positions()
    if open_positions:
        # Create orders.
        orders = []
        for position in open_positions:
            # Build an order to flatten the account.
            type_ = "market"
            curr_num_shares = float(position["info"]["positionAmt"])
            diff_num_shares = -curr_num_shares
            full_symbol = position["symbol"]
            asset_id = broker._symbol_to_asset_id_mapping[full_symbol]
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
            orders.append(order)
        asyncio.run(broker.submit_orders(orders, dry_run=dry_run))
    else:
        _LOG.warning("No open positions found.")
    # Check that all positions are closed.
    open_positions = broker.get_open_positions()
    if len(open_positions) != 0:
        _LOG.warning("Some positions failed to close: %s", open_positions)
    _LOG.info("Account flattened. Total balance: %s", broker.get_total_balance())


# #############################################################################
# Example Instances
# #############################################################################


def get_CcxtBroker_example1(
    market_data: mdata.MarketData,
    exchange_id: str,
    contract_type: str,
    stage: str,
    secret_id: int,
) -> occxbrok.CcxtBroker:
    """
    See `CcxtBroker` ctor for parameters description.

    :param exchange_id: name of exchange, e.g. "binance"
    :param contract_type: e.g. "futures"
    :param stage: e.g. "preprod"
    :return: initialized CCXT broker
    """
    # Set default broker values.
    universe = "v7.1"
    portfolio_id = "ccxt_portfolio_1"
    strategy_id = "C1b"
    account_type = "trading"
    secret_identifier = ohsseide.SecretIdentifier(
        exchange_id, stage, account_type, secret_id
    )
    # Initialize the broker.
    broker = occxbrok.CcxtBroker(
        exchange_id,
        universe,
        stage,
        account_type,
        portfolio_id,
        contract_type,
        secret_identifier,
        market_data=market_data,
        strategy_id=strategy_id,
    )
    return broker


# TODO(Dan): -> `get_MockCcxtRealTimeImClientMarketData`?
def get_RealTimeImClientMarketData_example2(
    im_client: icdc.RealTimeImClient,
) -> mdata.RealTimeMarketData2:
    """
    Create a RealTimeMarketData2 to use as placeholder in Broker.

    This example is geared to work with CcxtBroker.
    """
    asset_id_col = "asset_id"
    asset_ids = [1464553467]
    start_time_col_name = "start_timestamp"
    end_time_col_name = "end_timestamp"
    columns = None
    get_wall_clock_time = lambda: pd.Timestamp.now(tz="America/New_York")
    market_data = mdata.RealTimeMarketData2(
        im_client,
        asset_id_col,
        asset_ids,
        start_time_col_name,
        end_time_col_name,
        columns,
        get_wall_clock_time,
    )
    return market_data
