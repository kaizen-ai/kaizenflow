import logging

from invoke import task
import pandas as pd

import helpers.hio as hio
import market_data as mdata
import oms.ccxt_broker as occxbrok
import oms.hsecrets as omssec

_LOG = logging.getLogger(__name__)


def _get_CcxtBroker(secret_id: str) -> occxbrok.CcxtBroker:
    asset_ids = None
    market_data, _ =  mdata.get_ReplayedTimeMarketData_example3(asset_ids)
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
    _ = ctx
    ccxt_broker = _get_CcxtBroker(secret_id)
    open_positions = ccxt_broker.get_open_positions()
    df = pd.DataFrame(
        data=open_positions,
        columns=[
            "symbol", "side", "contracts", "contractSize", "notional"
        ]
    )
    _LOG.info("\n%s", df)
