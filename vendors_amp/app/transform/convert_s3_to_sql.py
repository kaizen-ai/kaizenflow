#!/usr/bin/env python
r"""
Converts data from S3 to SQL and inserts it into DB.

Usage examples:
- Convert daily data from S3 to SQL for AAPL, and provider "kibot":
  > convert_s3_to_sql.py \
      --provider kibot \
      --symbol AAPL \
      --frequency D \
      --contract_type continuous \
      --asset_class stocks \
      --exchange NYSE

- Convert daily data from S3 to SQL for AAPL, kibot provider, specifying connection:
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
import os

import helpers.dbg as dbg
import helpers.parser as hparse
import vendors_amp.app.services.loader_factory as vasloa
import vendors_amp.app.services.sql_writer_factory as vassql
import vendors_amp.app.services.transformer_factory as vastra
import vendors_amp.common.data.load.s3_data_loader as vcdls3
import vendors_amp.common.data.load.sql_data_loader as vcdlsq
import vendors_amp.common.data.transform.transform as vcdttr
import vendors_amp.common.data.types as vcdtyp

_LOG = logging.getLogger(__name__)


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
        type=vcdtyp.AssetClass,
        help="Asset class (e.g. Futures)",
        required=True,
    )
    parser.add_argument(
        "--frequency",
        type=vcdtyp.Frequency,
        help="Frequency of data (e.g. Minutely)",
        required=True,
    )
    parser.add_argument(
        "--contract_type",
        type=vcdtyp.ContractType,
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
    s3_data_loader: vcdls3.AbstractS3DataLoader = vasloa.LoaderFactory.get_loader(
        storage_type="s3", provider=provider
    )
    s3_to_sql_transformer = vastra.TransformerFactory.get_s3_to_sql_transformer(
        provider=provider
    )
    sql_writer_backend = vassql.SqlWriterFactory.get_sql_writer_backend(
        provider=provider,
        dbname=args.dbname,
        user=args.dbuser,
        password=args.dbpass,
        host=args.dbhost,
        port=args.dbport,
    )
    sql_data_loader: vcdlsq.AbstractSQLDataLoader = (
        vasloa.LoaderFactory.get_loader(
            storage_type="sql",
            provider=provider,
            dbname=args.dbname,
            user=args.dbuser,
            password=args.dbpass,
            host=args.dbhost,
            port=args.dbport,
        )
    )
    _LOG.info("Connecting to database")
    sql_writer_backend.ensure_exchange_exists(args.exchange)
    exchange_id = sql_data_loader.get_exchange_id(args.exchange)
    # Select symbols to process.
    symbols = args.symbol
    if args.max_num_assets is not None:
        _LOG.warning("Selected only %d symbols as per user request",
                     args.max_num_assets)
        dbg.dassert_lte(1, args.max_num_assets)
        symbols = symbols[: args.max_num_assets]
    # Construct list of parameters.
    params_list = []
    for symbol in symbols:
        params_list.append(
            dict(
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
            )
        )
    # Run converting.
    vcdttr.convert_s3_to_sql_bulk(serial=args.serial, params_list=params_list)
    _LOG.info("Closing database connection")
    sql_writer_backend.close()
    sql_data_loader.conn.close()


if __name__ == "__main__":
    _main(_parse())
