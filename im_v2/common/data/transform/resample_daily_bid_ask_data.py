#!/usr/bin/env python
"""
Load bid/ask parquet data from S3 exchange dir, resample to 1 minute and upload
back.

# Usage sample:
> im_v2/common/data/transform/resample_bid_ask_data.py \
    --start_timestamp '20220916-000000' \
    --end_timestamp '20220920-000000' \
    --src_dir 's3://bucket-name/reorg/daily_staged.airflow.pq/bid_ask/crypto_chassis.downloaded_1sec' \
    --dst_dir 's3://bucket-name/reorg/daily_staged.airflow.pq/bid_ask/crypto_chassis.resampled_1min'

Import as:

import im_v2.common.data.transform.resample_daily_bid_ask_data as imvcdtrdba
"""
import argparse
import logging

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.hparser as hparser
import im_v2.common.data.extract.extract_utils as imvcdeexut
import im_v2.common.data.transform.transform_utils as imvcdttrut

_LOG = logging.getLogger(__name__)


def _run(args: argparse.Namespace) -> None:
    aws_profile = "ck"
    epoch_unit = "s"
    # Convert dates to unix timestamps.
    start = hdateti.convert_timestamp_to_unix_epoch(
        pd.Timestamp(args.start_timestamp), unit=epoch_unit
    )
    end = hdateti.convert_timestamp_to_unix_epoch(
        pd.Timestamp(args.end_timestamp), unit=epoch_unit
    )
    # Define filters for data period.
    # Note(Juraj): it's better from Airflow execution perspective
    #  to keep the interval closed: [start, end].
    filters = [("timestamp", ">=", start), ("timestamp", "<=", end)]
    data = hparque.from_parquet(
        args.src_dir, filters=filters, aws_profile=aws_profile
    )
    # Ensure there are no duplicates, in this case
    #  using duplicates would compute the wrong resampled values.
    data = data.drop_duplicates(
        subset=["timestamp", "exchange_id", "currency_pair"]
    )
    data_resampled = []
    for currency_pair in data["currency_pair"].unique():
        data_single = data[data["currency_pair"] == currency_pair]
        if data_single.empty:
            _LOG.warning(
                "Empty Dataframe: no data for %s in %s-%s time period",
                currency_pair,
                args.start_timestamp,
                args.end_timestamp,
            )
            continue
        data_resampled_single = imvcdttrut.resample_multilevel_bid_ask_data(
            data_single
        )
        data_resampled_single["currency_pair"] = currency_pair
        data_resampled.append(data_resampled_single)
    # Transform the dataset to make save_parquet applicable.
    data_resampled = pd.concat(data_resampled).reset_index()
    data_resampled["timestamp"] = data_resampled["timestamp"].apply(
        lambda x: hdateti.convert_timestamp_to_unix_epoch(x, epoch_unit)
    )
    data_resampled = imvcdttrut.add_knowledge_timestamp_col(data_resampled, "UTC")
    _LOG.info(
        hpandas.df_to_str(
            data_resampled, print_shape_info=True, tag="Resampled data"
        )
    )
    imvcdeexut.save_parquet(
        data_resampled, args.dst_dir, epoch_unit, aws_profile, "bid_ask"
    )


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--src_dir",
        action="store",
        type=str,
        required=True,
        help="Path to exchange dir with input parquet files to resample to 1 minute frequency",
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        type=str,
        required=True,
        help="Destination dir where to save resampled parquet files",
    )
    parser.add_argument(
        "--start_timestamp",
        required=True,
        action="store",
        type=str,
        help="Beginning of the downloaded data period",
    )
    parser.add_argument(
        "--end_timestamp",
        action="store",
        required=True,
        type=str,
        help="End of the downloaded data period",
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _run(args)


if __name__ == "__main__":
    _main(_parse())
