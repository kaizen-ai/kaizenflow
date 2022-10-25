#!/usr/bin/env python
"""
Load bid/ask parquet data from S3 exchange dir, resample to 1 minute and upload
back.

# Usage sample:
> im_v2/common/data/transform/resample_bid_ask_data.py \
    --start_timestamp '20220916-000000' \
    --end_timestamp '20220920-000000' \
    --src_dir 's3://cryptokaizen-data-test/reorg/daily_staged.airflow.pq/bid_ask/crypto_chassis.downloaded_1sec' \
    --dst_dir 's3://cryptokaizen-data-test/reorg/daily_staged.airflow.pq/bid_ask/crypto_chassis.resampled_1min' 

Import as:

import im_v2.common.data.transform.resample_bid_ask_data as imvcdtrbad
"""
import argparse
import logging
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tqdm

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hparquet as hparque
import helpers.hparser as hparser
import helpers.hs3 as hs3
import im_v2.common.data.transform.transform_utils as imvcdttrut

_LOG = logging.getLogger(__name__)


def _run(args: argparse.Namespace) -> None:
    pattern = "*"
    only_files = True
    use_relative_paths = True
    aws_profile = "ck"
    # Get all files in the root dir.
    files_to_read = hs3.listdir(
        args.src_dir,
        pattern,
        only_files,
        use_relative_paths,
        aws_profile=aws_profile,
    )
    filesystem = hs3.get_s3fs(aws_profile)
    columns = [
        "bid_price",
        "bid_size",
        "ask_price",
        "ask_size",
        "exchange_id",
    ]
    # Convert dates to unix timestamps.
    start = hdateti.convert_timestamp_to_unix_epoch(
        pd.Timestamp(args.start_timestamp), unit="s" 
    )
    end = hdateti.convert_timestamp_to_unix_epoch(
        pd.Timestamp(args.end_timestamp), unit="s"
    )
    # Define filters for data period.
    filters = [("timestamp", ">=", start), ("timestamp", "<", end)]
    for file in tqdm.tqdm(files_to_read):
        file_path = os.path.join(args.src_dir, file)
        data = hparque.from_parquet(
            file_path, columns=columns, filters=filters, aws_profile=aws_profile
        )
        if data.empty:
            _LOG.warning(
                "Empty Dataframe: no data in %s for %s-%s time period",
                file_path,
                args.start_timestamp,
                args.end_timestamp,
            )
            continue
        data_resampled = imvcdttrut.resample_bid_ask_data(data)
        dst_path = os.path.join(args.dst_dir, file)
        data_resampled, partition_cols = hparque.add_date_partition_columns(
            data_resampled, "by_year_month"
        )
        hparque.to_partitioned_parquet(
            data_resampled,
            partition_cols,
            dst_path,
            partition_filename=None,
            aws_profile=aws_profile,
        )
        hparque.list_and_merge_pq_files(
            dst_path, 
            aws_profile=aws_profile, 
        )
        _LOG.info("Resampled data was uploaded to %s", args.dst_dir)


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
