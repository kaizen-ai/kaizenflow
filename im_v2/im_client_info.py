#!/usr/bin/env python
"""
Pass `--asset_id` param, e.g: --asset_id 3065029174, return full symbol.

Pass `--print_universe` param, e.g: --print_universe v5, return universe
as a list of asset ids.
"""
import argparse
import logging

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hsql as hsql
import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl
import im_v2.im_lib_tasks as imvimlita

_LOG = logging.getLogger(__name__)


def _get_ImClient():
    resample_1min = False
    env_file = imvimlita.get_db_env_path("dev")
    # Get login info.
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    # Login.
    db_connection = hsql.get_connection(*connection_params)
    # Get the real-time `ImClient`.
    table_name = "ccxt_ohlcv"
    im_client = imvcdccccl.CcxtSqlRealTimeImClient(
        resample_1min, db_connection, table_name
    )
    return im_client


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--print_universe",
        action="store",
        required=False,
        type=str,
        help="Universe version.",
    )
    parser.add_argument(
        "--asset_id",
        action="store",
        required=False,
        type=int,
        help="",
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _run(args: argparse.Namespace) -> None:
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    im_client = _get_ImClient()
    if args.asset_id:
        full_symbol = im_client.get_full_symbols_from_asset_ids([args.asset_id])
        _LOG.info("Full symbol: %s" % full_symbol)
    if args.print_universe:
        full_symbols = im_client.get_universe()
        asset_ids = im_client.get_asset_ids_from_full_symbols(full_symbols)
        _LOG.info("Asset ids: %s " % asset_ids)


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    _run(args)


if __name__ == "__main__":
    _main(_parse())
