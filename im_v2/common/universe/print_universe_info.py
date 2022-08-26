#!/usr/bin/env python
"""
The script performs several actions:

    - converts asset id to a full symbol
    - prints universe as a list of asset ids

The following command converts asset id to a full symbol:
    ```
    > im_v2/common/universe/print_universe_info.py \
        --action convert_to_full_symbol \
        --asset_id 3065029174 \
        --im_client ccxt_realtime
    ```

The command below prints the universe as asset ids:
    ```
    > im_v2/common/universe/print_universe_info.py \
        --action print_universe \
        --im_client ccxt_realtime
    ```
"""
import argparse
import logging

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hprint as hprint
import helpers.hsql as hsql
import im_v2.ccxt.data.client as icdcl
import im_v2.common.data.client as icdc
import im_v2.im_lib_tasks as imvimlita

_LOG = logging.getLogger(__name__)

_ACTIONS = ["print_universe", "convert_to_full_symbol"]


def _get_ImClient(im_client: str) -> icdc.ImClient:
    """
    Get `ImClient` from its string representation.

    :param im_client: client as string, e.g., `ccxt_realtime`
    """
    if im_client == "ccxt_realtime":
        resample_1min = False
        env_file = imvimlita.get_db_env_path("dev")
        # Get login info.
        connection_params = hsql.get_connection_info_from_env_file(env_file)
        # Login.
        db_connection = hsql.get_connection(*connection_params)
        # Get the real-time `ImClient`.
        table_name = "ccxt_ohlcv"
        #
        im_client = icdcl.CcxtSqlRealTimeImClient(
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
        choices=_ACTIONS,
        help=f"Choose action to perfom: {_ACTIONS}",
    )
    parser.add_argument(
        "--asset_id",
        action="store",
        required=False,
        type=int,
        help="Required if --action is convert_to_full_symbol",
    )
    parser.add_argument(
        "--im_client",
        action="store",
        required=True,
        type=str,
        help="",
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _run(args: argparse.Namespace) -> None:
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    im_client = _get_ImClient(args.im_client)
    if args.action == "convert_to_full_symbol":
        try:
            full_symbol = im_client.get_full_symbols_from_asset_ids(
                [args.asset_id]
            )
            _LOG.info("Full symbol: %s", full_symbol)
        # TODO(gp): Catch a stricter exception.
        except Exception as e:
            _LOG.error("Asset id is not a part of the universe or invalid: %s", e)
    if args.action == "print_universe":
        full_symbols = im_client.get_universe()
        asset_ids = im_client.get_asset_ids_from_full_symbols(full_symbols)
        # Print all asset ids.
        _LOG.info("Asset ids: %s ", hprint.format_list(asset_ids, max_n=100))


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    _run(args)


if __name__ == "__main__":
    _main(_parse())