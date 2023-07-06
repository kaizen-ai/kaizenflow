"""
Import as:

import oms.lib_tasks_binance as olitabin
"""

import logging

import pandas as pd
from invoke import task

import dataflow_amp.system.Cx as dtfamsysc
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hserver as hserver
import helpers.hsystem as hsystem
import oms.ccxt.ccxt_broker_instances as occcbrin
import oms.ccxt.ccxt_broker_v1 as occcbrv1
import oms.hsecrets as omssec

_LOG = logging.getLogger(__name__)


def _get_CcxtBroker(secret_id: int) -> occcbrv1.CcxtBroker_v1:
    """
    Get a `CcxtBroker` instance.
    """
    # `MarketData` is not strictly needed to talk to exchange, but since it is
    #  required to init the `Broker` we pass something to make it work.
    asset_ids = None
    market_data = dtfamsysc.get_Cx_RealTimeMarketData_prod_instance1(asset_ids)
    universe_version = "v7.1"
    strategy_id = "C1b"
    exchange_id = "binance"
    stage = "preprod"
    account_type = "trading"
    log_dir = "system_log_dir"
    secret_identifier = omssec.SecretIdentifier(
        exchange_id, stage, account_type, secret_id
    )
    ccxt_broker = occcbrin.get_CcxtBroker_v1_prod_instance1(
        strategy_id, market_data, universe_version, secret_identifier, log_dir
    )
    return ccxt_broker


@task
def binance_display_open_positions(ctx, secret_id):  # type: ignore
    """
    Get current open positions from binance and display in a human-readable
    format.

    :param secret_id: same as in `CcxtBroker`
    """
    hdbg.dassert(
        hserver.is_inside_docker(), "This is runnable only inside Docker."
    )
    _ = ctx
    hdbg.dassert(secret_id.isnumeric())
    secret_id = int(secret_id)
    ccxt_broker = _get_CcxtBroker(secret_id)
    open_positions = ccxt_broker.get_open_positions()
    columns = ["symbol", "side", "contracts", "contractSize", "notional"]
    df = pd.DataFrame(data=open_positions, columns=columns)
    df_str = hpandas.df_to_str(df, num_rows=None)
    _LOG.info("\n%s", df_str)


@task
def binance_log_open_positions(ctx, secret_id, log_dir):
    """
    Get all open positions and save to a logging directory.
    """
    hdbg.dassert(
        hserver.is_inside_docker(), "This is runnable only inside Docker."
    )
    _ = ctx
    hdbg.dassert(secret_id.isnumeric())
    secret_id = int(secret_id)
    exchange_id = "binance"
    contract_type = "futures"
    stage = "preprod"
    cmd = (
        f"oms/ccxt/scripts/get_ccxt_open_positions.py --exchange {exchange_id}"
        + f" --contract_type {contract_type} --stage {stage}"
        + f" --secret_id {secret_id}"
        + f" --log_dir {log_dir}"
    )
    hsystem.system(cmd, suppress_output=False)


@task
def binance_flatten_account(ctx, stage, secret_id):  # type: ignore
    """
    Close all open positions in a binance account.

    See CcxtBroker's ctor for parameters description.
    """
    hdbg.dassert(
        hserver.is_inside_docker(), "This is runnable only inside Docker."
    )
    _ = ctx
    hdbg.dassert(secret_id.isnumeric())
    secret_id = int(secret_id)
    exchange_id = "binance"
    contract_type = "futures"
    cmd = (
        f"oms/ccxt/scripts/flatten_ccxt_account.py --exchange_id {exchange_id}"
        + f" --contract_type {contract_type} --stage {stage}"
        + f" --secret_id {secret_id}"
        + f" --assert_on_non_zero_positions"
    )
    hsystem.system(cmd, suppress_output=False)


@task
def binance_log_total_balance(ctx, secret_id, log_dir):  # type: ignore
    """
    Get total balance and save to a logging directory.
    """
    hdbg.dassert(
        hserver.is_inside_docker(), "This is runnable only inside Docker."
    )
    _ = ctx
    hdbg.dassert(secret_id.isnumeric())
    secret_id = int(secret_id)
    exchange_id = "binance"
    contract_type = "futures"
    stage = "preprod"
    cmd = (
        f"oms/ccxt/scripts/get_ccxt_total_balance.py --exchange {exchange_id}"
        + f" --contract_type {contract_type} --stage {stage}"
        + f" --secret_id {secret_id}"
        + f" --log_dir {log_dir}"
    )
    hsystem.system(cmd, suppress_output=False)
