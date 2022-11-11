from invoke import task
import logging

import helpers.hsystem as hsystem
import market_data as mdata
import oms.ccxt_broker as occxbrok
import oms.hsecrets as omssec

_LOG = logging.getLogger(__name__)

def _system(cmd: str) -> int:
    return hsystem.system(cmd, suppress_output=False, log_level="echo")


def _get_CcxtBroker(secret_id: str) -> occxbrok.CcxtBroker:
    market_data = ""
    universe_version = "v7"
    strategy_id = "C1b"
    exchange_id = "binance"
    stage = "preprod"
    account_type = "trading"
    secret_identifier = omssec.SecretIdentifier(exchange_id, stage, account_type, secret_id)
    ccxt_broker = occxbrok.get_CcxtBroker_prod_instance1(
        market_data,
        universe_version,
        strategy_id,
        secret_identifier,
    )
    return ccxt_broker


@task
def get_open_positions(ctx, secret_id):
    _ = ctx
    ccxt_broker = _get_CcxtBroker(secret_id)
    open_positions = ccxt_broker.get_open_positions()
    file_name = "open_positions_from_binance.json"
    cmd = f"echo '{open_positions}' >> {file_name}"
    _system(cmd)

