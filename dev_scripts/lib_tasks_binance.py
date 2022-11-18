"""
Import as:

import dev_scripts.lib_tasks_binance as dslitabi
"""

import logging

import pandas as pd
from invoke import task

import dataflow_amp.system.Cx as dtfamsysc
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hserver as hserver
import oms.ccxt_broker as occxbrok
import oms.hsecrets as omssec

_LOG = logging.getLogger(__name__)


def _get_CcxtBroker(secret_id: int) -> occxbrok.CcxtBroker:
    """
    Get a `CcxtBroker` instance.
    """
    # `MarketData` is not strictly needed to talk to exchange, but since it is
    #  required to init the `Broker` we pass something to make it work.
    asset_ids = None
    market_data = dtfamsysc.get_Cx_RealTimeMarketData_prod_instance1(asset_ids)
    universe_version = "v7"
    strategy_id = "C1b"
    exchange_id = "binance"
    stage = "preprod"
    account_type = "trading"
    secret_identifier = omssec.SecretIdentifier(
        exchange_id, stage, account_type, secret_id
    )
    ccxt_broker = occxbrok.get_CcxtBroker_prod_instance1(
        market_data,
        universe_version,
        strategy_id,
        secret_identifier,
    )
    return ccxt_broker


@task
def binance_get_open_positions(ctx, secret_id):
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
    df = pd.DataFrame(
        data=open_positions,
        columns=columns,
    )
    df_str = hpandas.df_to_str(df, num_rows=None)
    _LOG.info("\n%s", df_str)
