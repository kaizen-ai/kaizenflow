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
import vendors_amp.kibot.sql_writer_backend as vdsqlw
import vendors_amp.kibot.data.config as vkdcon
import vendors_amp.kibot.data.load as vkdloa
import vendors_amp.kibot.data.load.dataset_name_parser as vkdlda
import vendors_amp.kibot.data.load.sql_data_loader as vkdlsq
import vendors_amp.kibot.data.types as vkdtyp
import vendors_amp.kibot.metadata.load.s3_backend as vkmls3

_LOG = logging.getLogger(__name__)

_JOBLIB_NUM_CPUS = 10
_JOBLIB_VERBOSITY = 1

# #############################################################################


def _convert_kibot_csv_gz_to_sql(
    symbol: str,
    exchange: str,
    kibot_data_loader: vkdloa.S3KibotDataLoader,
    sql_writer_backed: vdsqlw.SQLWriterBackend,
    sql_data_loader: vkdlsq.SQLKibotDataLoader,
    asset_class: vkdtyp.AssetClass,
    frequency: vkdtyp.Frequency,
    exchange_id: int,
    contract_type: Optional[vkdtyp.ContractType] = None,
    unadjusted: Optional[bool] = None,
    max_num_rows: Optional[int] = None,
) -> bool:
    """
    Convert a Kibot dataset for a symbol.

    :return: True if it was processed
    """
    _LOG.debug("Managing database for '%s' symbol", symbol)
    sql_writer_backed.ensure_symbol_exists(symbol=symbol, asset_class=asset_class)
    symbol_id = sql_data_loader.get_symbol_id(symbol=symbol)
    sql_writer_backed.ensure_trade_symbol_exists(
        symbol_id=symbol_id, exchange_id=exchange_id
    )
    trade_symbol_id = sql_data_loader.get_trade_symbol_id(
        symbol_id=symbol_id, exchange_id=exchange_id
    )
    _LOG.info("Converting '%s' symbol", symbol)
    _LOG.debug("Downloading '%s' symbol from S3", symbol)
    df = kibot_data_loader.read_data(
        exchange=exchange,
        symbol=symbol,
        asset_class=asset_class,
        frequency=frequency,
        contract_type=contract_type,
        unadjusted=unadjusted,
        normalize=False,
    )
    if max_num_rows:
        df = df.head(max_num_rows)
    if frequency == vkdtyp.Frequency.Minutely:
        df.columns = [
            "date",
            "time",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        # Transform DataFrame from S3 to DB format.
        df["trade_symbol_id"] = trade_symbol_id
        df["datetime"] = df["date"].str.cat(df["time"], sep=" ")
        del df["date"]
        del df["time"]
        sql_writer_backed.insert_bulk_minute_data(df)
    elif frequency == vkdtyp.Frequency.Daily:
        df.columns = ["date", "open", "high", "low", "close", "volume"]
        # Transform DataFrame from S3 to DB format.
        df["trade_symbol_id"] = trade_symbol_id
        sql_writer_backed.insert_bulk_daily_data(df)
    elif frequency == vkdtyp.Frequency.Tick:
        df.columns = ["date", "time", "price", "size"]
        for _, row in df.iterrows():
            sql_writer_backed.insert_tick_data(
                trade_symbol_id=trade_symbol_id,
                date_time=f"${row['date']} ${row['time']}",
                price_val=row["price"],
                size_val=row["size"],
            )
    else:
        dbg.dfatal("Unknown frequency '%s'", frequency)
    _LOG.info("Done converting '%s' symbol", symbol)
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
            fn(symbol=symbol, **kwargs)
    else:
        joblib.Parallel(
            n_jobs=_JOBLIB_NUM_CPUS,
            verbose=_JOBLIB_VERBOSITY,
            require="sharedmem",
        )(joblib.delayed(fn)(symbol=symbol, **kwargs) for symbol in tqdm_)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--serial",
        action="store_true",
        help="Download data serially",
    )
    parser.add_argument(
        "--dataset",
        type=str,
        help="Process a specific dataset (or all datasets if omitted)",
        choices=vkdcon.DATASETS,
        action="append",
        default=None,
    )
    parser.add_argument(
        "--exchange",
        type=str,
        help="Selected Exchange",
        required=True,
        default=None,
    )
    parser.add_argument(
        "--dbuser",
        type=str,
        help="Postgres User",
        default=None,
    )
    parser.add_argument(
        "--dbpass",
        type=str,
        help="Postgres Password",
        default=None,
    )
    parser.add_argument(
        "--dbhost",
        type=str,
        help="Postgres Host",
        required=True,
        default=None,
    )
    parser.add_argument(
        "--dbname",
        type=str,
        help="Postgres DB",
        required=True,
        default="postgres",
    )
    parser.add_argument(
        "--max_num_assets",
        action="store",
        type=int,
        default=None,
        help="Maximum number of assets to copy (for debug)",
    )
    parser.add_argument(
        "--max_num_rows",
        action="store",
        type=int,
        default=None,
        help="Maximum number of rows per asset to copy (for debug)",
    )
    hparse.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    dbg.shutup_chatty_modules()
    #
    kibot_data_loader = vkdloa.S3KibotDataLoader()
    #
    s3_backend = vkmls3.S3Backend()
    #
    dataset_name_parser = vkdlda.DatasetNameParser()
    #
    sql_writer_backed = vdsqlw.SQLWriterBackend(
        dbname=args.dbname,
        user=args.dbuser,
        password=args.dbpass,
        host=args.dbhost,
    )
    #
    sql_data_loader = vkdlsq.SQLKibotDataLoader(
        dbname=args.dbname,
        user=args.dbuser,
        password=args.dbpass,
        host=args.dbhost,
    )
    _LOG.info("Connected to database")
    #
    exchange_id = sql_data_loader.get_exchange_id(args.exchange)
    dbg.dassert_lte(0, exchange_id, f"Exchange '{args.exchange}' does not exist.")
    # Go over selected datasets or all datasets.
    datasets_to_process = args.dataset or vkdcon.DATASETS
    _LOG.info("Processing %d datasets", len(datasets_to_process))
    for dataset in tqdm.tqdm(datasets_to_process, desc="dataset"):
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
            max_num_rows=args.max_num_rows,
            kibot_data_loader=kibot_data_loader,
            sql_writer_backed=sql_writer_backed,
            sql_data_loader=sql_data_loader,
            asset_class=asset_class,
            contract_type=contract_type,
            frequency=frequency,
            unadjusted=unadjusted,
            exchange_id=exchange_id,
            exchange=args.exchange,
        )
        #
    _LOG.info("Closing database connection")
    sql_writer_backed.close()


if __name__ == "__main__":
    _main(_parse())
