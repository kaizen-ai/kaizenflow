#!/usr/bin/env python
r"""
Converts data from S3 to SQL and inserts it into DB.

Usage:
    1. Convert daily data from S3 to SQL for AAPL, kibot provider:
    > convert_s3_to_sql.py \
        --provider kibot \
        --symbol AAPL \
        --frequency D \
        --contract_type continuous \
        --asset_class stocks \
        --exchange NYSE

    2. Convert daily data from S3 to SQL for AAPL, kibot provider,
        specifying connection:
    > convert_s3_to_sql.py \
        --provider kibot \
        --symbol AAPL \
        --frequency D \
        --contract_type continuous \
        --asset_class stocks \
        --exchange NYSE \
        --dbname kibot_postgres_db_local \
        --dbhost kibot_postgres_local \
        --dbuser menjgbcvejlpcbejlc \
        --dbpass eidvlbaresntlcdbresntdjlrs \
        --dbport 5432

"""

import argparse
import logging
from typing import Any, Callable, List, Optional, Dict
import os

import joblib
import tqdm

import helpers.dbg as dbg
import helpers.parser as hparse
import helpers.printing as prnt
import vendors_amp.common.data.types as vkdtyp
import vendors_amp.kibot.sql_writer_backend as vksqlw
import vendors_amp.kibot.data.load.s3_data_loader as mds3
import vendors_amp.kibot.data.load.sql_data_loader as mdsql
import vendors_amp.common.data.transform.s3_to_sql_transformer as mtra
import vendors_amp.common.data.transform.transformer_factory as tfac

_LOG = logging.getLogger(__name__)

_JOBLIB_NUM_CPUS = 10
_JOBLIB_VERBOSITY = 1

# #############################################################################


def convert_s3_to_sql(
    symbol: str,
    exchange: str,
    s3_data_loader: mds3.S3KibotDataLoader,
    sql_writer_backend: vksqlw.SQLWriterBackend,
    sql_data_loader: mdsql.SQLKibotDataLoader,
    s3_to_sql_transformer: mtra.AbstractS3ToSqlTransformer,
    asset_class: vkdtyp.AssetClass,
    frequency: vkdtyp.Frequency,
    exchange_id: int,
    contract_type: Optional[vkdtyp.ContractType] = None,
    unadjusted: Optional[bool] = None,
    max_num_rows: Optional[int] = None,
) -> bool:
    """
    Convert a dataset from S3 for a symbol.

    :return: True if it was processed
    """
    _LOG.debug("Managing database for '%s' symbol", symbol)
    sql_writer_backend.ensure_symbol_exists(symbol=symbol, asset_class=asset_class)
    symbol_id = sql_data_loader.get_symbol_id(symbol=symbol)
    sql_writer_backend.ensure_trade_symbol_exists(
        symbol_id=symbol_id, exchange_id=exchange_id
    )
    trade_symbol_id = sql_data_loader.get_trade_symbol_id(
        symbol_id=symbol_id, exchange_id=exchange_id
    )
    _LOG.info("Converting '%s' symbol", symbol)
    _LOG.debug("Downloading '%s' symbol from S3", symbol)
    df = s3_data_loader.read_data(
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
    _LOG.debug("Transforming '%s' data before saving to database", symbol)
    df = s3_to_sql_transformer.transform(df, trade_symbol_id=trade_symbol_id, frequency=frequency)
    _LOG.debug("Saving '%s' data to database", symbol)
    if frequency == vkdtyp.Frequency.Minutely:
        sql_writer_backend.insert_bulk_minute_data(df)
    elif frequency == vkdtyp.Frequency.Daily:
        sql_writer_backend.insert_bulk_daily_data(df)
    elif frequency == vkdtyp.Frequency.Tick:
        for _, row in df.iterrows():
            sql_writer_backend.insert_tick_data(
                trade_symbol_id=row["trade_symbol_id"],
                date_time=row["datetime"],
                price_val=row["price"],
                size_val=row["size"],
            )
    else:
        dbg.dfatal("Unknown frequency '%s'", frequency)
    _LOG.info("Done converting '%s' symbol", symbol)
    # Return info about loaded data.
    loaded_data = sql_data_loader.read_data(exchange=exchange, symbol=symbol, asset_class=asset_class, frequency=frequency, contract_type=contract_type, unadjusted=unadjusted, nrows=None, normalize=True)
    _LOG.info("Total %s records loaded for symbol '%s'", len(loaded_data), symbol)
    _LOG.debug("Tail of loaded data:\n%s", prnt.frame(loaded_data.tail().to_string()))
    return True


def convert_s3_to_sql_bulk(
    serial: bool, params_list: List[Dict[str, Any]]
) -> None:
    """
    Process in parallel each set of params in the list.

    :param serial: whether to run sequentially
    :param params_list: list of parameters to run
    """
    tqdm_ = tqdm.tqdm(params_list, desc="symbol", total=len(params_list))
    if serial:
        for params in tqdm_:
            convert_s3_to_sql(**params)
    else:
        joblib.Parallel(
            n_jobs=_JOBLIB_NUM_CPUS,
            verbose=_JOBLIB_VERBOSITY,
            require="sharedmem",
        )(joblib.delayed(convert_s3_to_sql)(**params) for params in tqdm_)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--provider",
        type=str,
        help="Provider to transform",
        action="store",
        required=True,
    )
    parser.add_argument(
        "--serial",
        action="store_true",
        help="Download data serially",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        help="Symbols to process",
        action="append",
        required=True,
    )
    parser.add_argument(
        "--exchange",
        type=str,
        help="Selected Exchange",
        required=True,
    )
    parser.add_argument(
        "--asset_class",
        type=vkdtyp.AssetClass,
        help="Asset class (e.g. Futures)",
        required=True,
    )
    parser.add_argument(
        "--frequency",
        type=vkdtyp.Frequency,
        help="Frequency of data (e.g. Minutely)",
        required=True,
    )
    parser.add_argument(
        "--contract_type",
        type=vkdtyp.ContractType,
        help="Contract type (e.g. Expiry)",
        required=True,
    )
    parser.add_argument(
        "--unadjusted",
        action="store_true",
        help="Set if data is unadjusted in S3",
    )
    parser.add_argument(
        "--dbuser",
        type=str,
        help="Postgres User",
        default=os.environ.get("POSTGRES_USER", None),
    )
    parser.add_argument(
        "--dbpass",
        type=str,
        help="Postgres Password",
        default=os.environ.get("POSTGRES_PASSWORD", None),
    )
    parser.add_argument(
        "--dbhost",
        type=str,
        help="Postgres Host",
        default=os.environ.get("POSTGRES_HOST", None),
    )
    parser.add_argument(
        "--dbport",
        type=int,
        help="Postgres Port",
        default=os.environ.get("POSTGRES_PORT", None),
    )
    parser.add_argument(
        "--dbname",
        type=str,
        help="Postgres DB",
        default=os.environ.get("POSTGRES_DB", None),
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
    # Set up parameters for running.
    provider = args.provider
    s3_to_sql_transformer = tfac.TransformerFactory.get_s3_to_sql_transformer(provider=provider)
    # TODO(plyq): remove kibot specific.
    s3_data_loader = mds3.S3KibotDataLoader()
    sql_writer_backend = vksqlw.SQLWriterBackend(
        dbname=args.dbname,
        user=args.dbuser,
        password=args.dbpass,
        host=args.dbhost,
        port=args.dbport,
    )
    # TODO(plyq): remove kibot specific.
    sql_data_loader = mdsql.SQLKibotDataLoader(
        dbname=args.dbname,
        user=args.dbuser,
        password=args.dbpass,
        host=args.dbhost,
        port=args.dbport,
    )
    _LOG.info("Connected to database")
    sql_writer_backend.ensure_exchange_exists(args.exchange)
    exchange_id = sql_data_loader.get_exchange_id(args.exchange)
    # Select symbols to process.
    symbols = args.symbol
    if args.max_num_assets is not None:
        dbg.dassert_lte(1, args.max_num_assets)
        symbols = symbols[: args.max_num_assets]
    # Construct list of parameters.
    params_list = []
    for symbol in symbols:
        params_list.append(dict(
            symbol=symbol,
            max_num_rows=args.max_num_rows,
            s3_data_loader=s3_data_loader,
            sql_writer_backend=sql_writer_backend,
            sql_data_loader=sql_data_loader,
            s3_to_sql_transformer=s3_to_sql_transformer,
            asset_class=args.asset_class,
            contract_type=args.contract_type,
            frequency=args.frequency,
            unadjusted=args.unadjusted,
            exchange_id=exchange_id,
            exchange=args.exchange,
        ))
    # Run converting.
    convert_s3_to_sql_bulk(serial=args.serial, params_list=params_list)
    _LOG.info("Closing database connection")
    sql_writer_backend.close()
    sql_data_loader.conn.close()


if __name__ == "__main__":
    _main(_parse())
