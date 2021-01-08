#!/usr/bin/env python
r"""
Converts Kibot data on S3 from .csv.gz to SQL and inserts it into DB.
"""

import argparse
import logging
from typing import Any, Callable, List, Optional

import joblib
import tqdm

import helpers.dbg as dbg
import helpers.parser as hparse
import vendors2.docker.sql_writter_backend as vdsqlw
import vendors2.kibot.data.config as vkdcon
import vendors2.kibot.data.load as vkdloa
import vendors2.kibot.data.load.dataset_name_parser as vkdlda
import vendors2.kibot.data.types as vkdtyp
import vendors2.kibot.metadata.load.s3_backend as vkmls3

_LOG = logging.getLogger(__name__)

_JOBLIB_NUM_CPUS = 10
_JOBLIB_VERBOSITY = 1

# #############################################################################


def _convert_kibot_csv_gz_to_sql(
    symbol: str,
    kibot_data_loader: vkdloa.KibotDataLoader,
    sql_writer_backed: vdsqlw.SQLWriterBackend,
    asset_class: vkdtyp.AssetClass,
    frequency: vkdtyp.Frequency,
    exchange_id: int,
    contract_type: Optional[vkdtyp.ContractType] = None,
    unadjusted: Optional[bool] = None,
) -> bool:
    """
    Convert a Kibot dataset for a symbol.

    :return: True if it was processed
    """
    df = kibot_data_loader.read_data(
        symbol=symbol,
        asset_class=asset_class,
        frequency=frequency,
        contract_type=contract_type,
        unadjusted=unadjusted,
        normalize=False,
    )
    sql_writer_backed.ensure_symbol_exists(symbol=symbol, asset_class=asset_class)
    symbol_id = sql_writer_backed.get_symbol_id(symbol=symbol)
    sql_writer_backed.ensure_trade_symbol_exists(
        symbol_id=symbol_id, exchange_id=exchange_id
    )
    trade_symbol_id = sql_writer_backed.get_trade_symbol_id(
        symbol_id=symbol_id, exchange_id=exchange_id
    )
    # TODO(vr): Below only for Minute frequency.
    df.columns = ["date", "time", "open", "high", "low", "close", "volume"]
    for _, row in df.iterrows():
        sql_writer_backed.insert_minute_data(
            trade_symbol_id=trade_symbol_id,
            datetime=f"${row['date']} ${row['time']}",
            open_val=row["open"],
            high_val=row["high"],
            low_val=row["low"],
            close_val=row["close"],
            volume_val=row["volume"],
        )
    return True


def _process_over_dataset(
    fn: Callable, symbols: List[str], serial: bool, **kwargs: Any
) -> None:
    """
    Process in parallel each symbol in the list.

    :param fn: a procedure to be run for each symbol
    :param symbols: list of symbols to run fn over
    :param serial: whether to run sequentially
    :param kwargs: other arguments to pass to fn
    """
    tqdm_ = tqdm.tqdm(symbols, desc="symbol", total=len(symbols))
    if serial:
        for symbol in tqdm_:
            print(symbol)
            fn(symbol=symbol, **kwargs)
    else:
        joblib.Parallel(n_jobs=_JOBLIB_NUM_CPUS, verbose=_JOBLIB_VERBOSITY)(
            joblib.delayed(fn)(symbol=symbol, **kwargs) for symbol in tqdm_
        )


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--serial",
        # TODO(vr): Change this back.
        # action="store_true",
        action="store_false",
        help="Download data serially",
    )
    parser.add_argument(
        "--dataset",
        type=str,
        help="Process a specific dataset (or all datasets if omitted)",
        choices=vkdcon.S3_DATASETS,
        action="append",
        # TODO(vr): Change this back.
        # default=None,
        default=["All_Futures_Continuous_Contracts_1min"],
    )
    parser.add_argument(
        "--exchange",
        type=str,
        help="Selected Exchange",
        # TODO(vr): Change this back.
        # default=None,
        default="TestExchange",
    )
    parser.add_argument(
        "--dbuser",
        type=str,
        help="Postgres User",
        # TODO(vr): Change this back.
        # default=None,
        default="postgres",
    )
    parser.add_argument(
        "--dbpass",
        type=str,
        help="Postgres Password",
        # TODO(vr): Change this back.
        # required=True,
        # default=None,
        default="password",
    )
    parser.add_argument(
        "--dbhost",
        type=str,
        help="Postgres Host",
        # TODO(vr): Change this back.
        # required=True,
        # default=None,
        default="127.0.0.1",
    )
    parser.add_argument(
        "--dbname",
        type=str,
        help="Postgres DB",
        # TODO(vr): Change this back.
        # required=True,
        # default=None,
        default="postgres",
    )
    parser.add_argument(
        "--max_num_assets",
        action="store",
        type=int,
        # TODO(vr): Change this back.
        # default=None,
        default=2,
        help="Maximum number of assets to copy (for debug)",
    )
    hparse.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    dbg.shutup_chatty_modules()
    # Create Kibot Downloader instance.
    kibot_data_loader = vkdloa.KibotDataLoader()
    # Create S3 Backend instance.
    s3_backend = vkmls3.S3Backend()
    # Create Dataset Name Parser instance.
    dataset_name_parser = vkdlda.DatasetNameParser()
    # Connect to PSQL.
    sql_writer_backed = vdsqlw.SQLWriterBackend(
        dbname=args.dbname,
        user=args.dbuser,
        password=args.dbpass,
        host=args.dbhost,
    )
    _LOG.info("Connected to database")
    #
    exchange_id = sql_writer_backed.get_exchange_id(args.exchange)
    # Go over selected datasets or all datasets.
    datasets_to_proceed = args.dataset or vkdcon.S3_DATASETS
    _LOG.info("Proceeding %d datasets", len(datasets_to_proceed))
    for dataset in tqdm.tqdm(datasets_to_proceed, desc="dataset"):
        # Get the symbols from S3.
        symbols = s3_backend.get_symbols_for_dataset(dataset)
        # symbols = ["AAPL"]
        if args.max_num_assets is not None:
            dbg.dassert_lte(1, args.max_num_assets)
            symbols = symbols[: args.max_num_assets]
        _LOG.info("Found %d symbols", len(symbols))
        # Parse dataset name and extract parameters.
        (
            asset_class,
            contract_type,
            frequency,
            unadjusted,
        ) = dataset_name_parser.parse_dataset_name(dataset)
        #
        _process_over_dataset(
            _convert_kibot_csv_gz_to_sql,
            symbols,
            args.serial,
            kibot_data_loader=kibot_data_loader,
            sql_writer_backed=sql_writer_backed,
            asset_class=asset_class,
            contract_type=contract_type,
            frequency=frequency,
            unadjusted=unadjusted,
            exchange_id=exchange_id,
        )
        #
    _LOG.info("Closing database connection")
    sql_writer_backed.close()


if __name__ == "__main__":
    _main(_parse())
