#!/usr/bin/env python
"""
Load raw bid/ask data from specified DB table every minute, resample to 1
minute and insert back in a specified time interval. Currently resamples only
top of the book.

# Usage sample:
> im_v2/common/data/transform/resample_rt_bid_ask_data_periodically.py \
    --db_stage 'test' \
    --dag_signature 'realtime.airflow.downloaded_200ms.postgres.bid_ask.futures.v7_5.binance.binance.v1_0_0' \
    --start_ts '2022-10-12 16:05:05+00:00' \
    --end_ts '2022-10-12 18:30:00+00:00' \
    --resample_freq '1T'
"""
import argparse
import logging
from typing import Any, Dict

import pandas as pd

import data_schema.dataset_schema_utils as dsdascut
import helpers.hdbg as hdbg
import helpers.hparser as hparser
import im_v2.common.data.extract.extract_utils as imvcdeexut
import im_v2.common.universe.universe as imvcounun

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--db_stage",
        action="store",
        required=True,
        type=str,
        help="DB stage to use",
    )
    parser.add_argument(
        "--dag_signature",
        action="store",
        type=str,
        required=True,
        help="Signature to extract table names and other parameters",
    )
    parser.add_argument(
        "--start_ts",
        required=True,
        action="store",
        type=str,
        help="Beginning of the resample data period",
    )
    parser.add_argument(
        "--end_ts",
        action="store",
        required=True,
        type=str,
        help="End of the resampled data period",
    )
    parser.add_argument(
        "--resample_freq",
        action="store",
        required=True,
        type=str,
        help='Frequency that we want to resample the data to, e.g., "10S", "1T"',
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _build_table_name_from_signature(
    signature: str, dataset_schema: Dict[str, Any], freq: str
) -> tuple[str, str]:
    """
    Build source and destination table names from the signature.

    :param signature: dataset signature to resample,
    :param dataset_schema: dataset schema to parse against
    :param freq: resampling freq
    :return: tuple of source table name and destination table name
    """
    freq_str = freq.replace("T", "min").replace("S", "sec")
    # Construct the src and dst table names.
    src_table = dsdascut.get_im_db_table_name_from_signature(
        signature, dataset_schema
    )
    # Check that valid dst table suffix is generated.
    dst_suffix = f"resampled_{freq_str}"
    hdbg.dassert_in(dst_suffix, dataset_schema["allowed_values"]["action_tag"])
    # Bid ask table has `_raw` keyword in the table name, replace that with resampled_1min.
    dst_table = src_table.replace("_raw", f"_{dst_suffix}")
    return (src_table, dst_table)


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    args = vars(args)
    dataset_schema = dsdascut.get_dataset_schema()
    # Parse the signature.
    signature_params = dsdascut.parse_dataset_signature_to_args(
        args["dag_signature"], dataset_schema
    )
    mode = "download"
    version = signature_params["universe"].replace("_", ".")
    vendor_name = signature_params["vendor"]
    exchange_id = signature_params["exchange_id"]
    universe = imvcounun.get_vendor_universe(vendor_name, mode, version=version)
    currency_pairs = universe[exchange_id]
    db_stage = args["db_stage"]
    resample_freq = args["resample_freq"]
    src_table, dst_table = _build_table_name_from_signature(
        args["dag_signature"], dataset_schema, resample_freq
    )
    _LOG.info("Resampling bid/ask data for %s", currency_pairs)
    imvcdeexut.resample_rt_bid_ask_data_periodically(
        db_stage,
        src_table,
        dst_table,
        exchange_id,
        resample_freq,
        pd.Timestamp(args["start_ts"]),
        pd.Timestamp(args["end_ts"]),
        currency_pairs=currency_pairs,
    )


if __name__ == "__main__":
    _main(_parse())
