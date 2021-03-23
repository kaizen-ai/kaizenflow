#!/usr/bin/env python

import argparse
import logging

import pandas as pd

import helpers.dbg as dbg
import helpers.parser as hparse
import vendors_amp.ib.extract.download_data_ib_loop as viedow
import vendors_amp.ib.extract.utils as vieuti

# from tqdm.notebook import tqdm

_LOG = logging.getLogger(__name__)


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    dbg.shutup_chatty_modules()
    #
    if False:
        target = "forex"
        frequency = "intraday"
        symbols = ["EURUSD"]
    elif False:
        target = "futures"
        frequency = "intraday"
        symbols = ["ES"]
    elif False:
        df = pd.read_csv("./ib_futures.csv")
        # print(df)
        symbols = df["IB SYMBOL"][:10]
    # Set up.
    symbols = [args.symbol]
    args.exchange
    target = args.asset_class
    frequency = args.frequency
    ib = vieuti.ib_connect(0, is_notebook=False)
    use_rth = False
    start_ts = None
    end_ts = None
    num_threads = "serial"
    dst_dir = args.dst_dir
    incremental = args.incremental
    client_id_base = 5
    tasks = vieuti.get_tasks(
        ib, target, frequency, symbols, start_ts, end_ts, use_rth
    )
    file_names = viedow.download_ib_data(
        client_id_base, tasks, incremental, dst_dir, num_threads
    )
    _LOG.info("file_names=%s", file_names)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("positional", nargs="*", help="...")
    parser.add_argument(
        "--dst_dir",
        action="store",
        default="./tmp.download_data",
        help="Destination dir",
    )
    parser.add_argument(
        "--symbol", action="store", required=True, help="Symbol ticker"
    )
    parser.add_argument(
        "--exchange",
        action="store",
        required=True,
        help="Symbol trading exchange",
    )
    parser.add_argument(
        "--asset_class",
        action="store",
        required=True,
        help="Asset class of the symbol",
    )
    parser.add_argument(
        "--frequency",
        action="store",
        required=True,
        help="intraday or day or hour",
    )
    parser.add_argument("--incremental", action="store_true", default=True)
    hparse.add_verbosity_arg(parser)
    return parser


if __name__ == "__main__":
    _main(_parse())
