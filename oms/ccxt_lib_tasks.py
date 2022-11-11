import logging

import helpers.hsystem as hsystem
import oms.ccxt_broker as occxbrok
import oms.hsecrets as omssec

_LOG = logging.getLogger(__name__)

def _system(cmd: str) -> int:
    return hsystem.system(cmd, suppress_output=False, log_level="echo")


def _get_CcxtBroker() -> occxbrok.CcxtBroker:
    exchange_id = "binance"
    universe_version = "v7"
    stage = "preprod"
    account_type = "trading"
    portfolio_id = "ccxt_portfolio_1"
    contract_type = "futures"
    secret_identifier = omssec.SecretIdentifier
    ccxt_broker = occxbrok.CcxtBroker(
        exchange_id,
        universe_version,
        stage,
        account_type,
        portfolio_id,
        contract_type,
        secret_identifier
    )
    return ccxt_broker


def get_current_open_positions_from_binance(ctx):
    _ = ctx
    ccxt_broker = _get_CcxtBroker()
    open_positions = ccxt_broker.get_open_positions()
    file_name = "open_positions_from_binance.json"
    cmd = f"echo '{open_positions}' >> {file_name}"
    _system(cmd)

