"""
Import as:

import oms.submit_twap_orders as osutword
"""

import argparse
import logging

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import im_v2.common.db.db_utils as imvcddbut
import im_v2.crypto_chassis.data.client as iccdc
import oms.oms_ccxt_utils as oomccuti

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Get connection.
    imvcddbut.DbConnectionManager.get_connection("dev")
    # Get IM client for bid/ask data.
    # TODO(Danya): get latest universe, provide contract type as argument.
    # TODO(Danya): pass full symbols as arguments.
    universe_version = "v3"
    resample_1min = False
    contract_type = "futures"
    tag = "downloaded_1sec"
    im_client = iccdc.get_CryptoChassisHistoricalPqByTileClient_example2(
        universe_version, resample_1min, contract_type, tag
    )
    # Initialize Market Data.
    market_data = oomccuti.get_RealTimeImClientMarketData_example2(im_client)
    # Initialize Broker.
    exchange_id = "binance"
    stage = "preprod"
    secret_id = 2
    broker = oomccuti.get_CcxtBroker_example1(
        market_data, exchange_id, contract_type, stage, secret_id
    )
    broker.create_twap_orders()
    # TODO(Danya): submit orders (create submit_twap_orders).


if __name__ == "__main__":
    _main(_parse())
