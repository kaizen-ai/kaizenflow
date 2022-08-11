#!/usr/bin/env python
"""
Return information about:

- full symbol
    E.g: `> im_client_info.py --action asset_id --param 3065029174 --im_client ccxt_realtime`
- universe as a list of asset ids
    E.g: `> im_client_info.py --action print_universe --im_client ccxt_realtime`
"""
import argparse
import logging

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hsql as hsql
import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl
import im_v2.common.data.client.base_im_clients as imvcdcbimcl
import im_v2.im_lib_tasks as imvimlita

_LOG = logging.getLogger(__name__)

ACTIONS = ["print_universe", "asset_id"]


def _get_ImClient(im_client: str) -> imvcdcbimcl.SqlRealTimeImClient:
    if im_client == "ccxt_realtime":
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
    else:
        raise ValueError(f"Invalid im_client='{im_client}'.")
    return im_client


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--action",
        action="store",
        required=True,
        type=str,
        choices=ACTIONS,
        help=f"Choose action to perfom: {ACTIONS}",
    )
    parser.add_argument(
        "--param",
        action="store",
        required=False,
        type=int,
        help="Required if --action asset_id",
    )
    parser.add_argument(
        "--im_client",
        default="ccxt_realtime",
        action="store",
        required=False,
        type=str,
        help="",
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _run(args: argparse.Namespace) -> None:
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    im_client = _get_ImClient(args.im_client)
    if args.action == "asset_id":
        try:
            full_symbol = im_client.get_full_symbols_from_asset_ids([args.param])
            _LOG.info("Full symbol: %s" % full_symbol)
        except Exception:
            _LOG.error("Asset id is missing. Add as --param value.")
    if args.action == "print_universe":
        full_symbols = im_client.get_universe()
        asset_ids = im_client.get_asset_ids_from_full_symbols(full_symbols)
        _LOG.info("Asset ids: %s " % asset_ids)


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    _run(args)


if __name__ == "__main__":
    _main(_parse())
