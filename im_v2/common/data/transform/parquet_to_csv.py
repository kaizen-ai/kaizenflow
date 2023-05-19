import argparse
import logging
import os

import pandas as pd


import helpers.hdbg as hdbg
import helpers.hparquet as hparque
import helpers.hparser as hparser
import helpers.hs3 as hs3
import im_v2.common.data.transform.transform_utils as imvcdttrut

_LOG = logging.getLogger(__name__)

def _run(args: argparse.Namespace) -> None:
    if args.aws_profile is not None:
        hs3.get_s3fs(args.aws_profile)
    else:
        hparser.create_incremental_dir(args.dst_dir, args)
    files = hs3.listdir(
        args.src_dir,
        "*.pq*",
        only_files=True,
        use_relative_paths=True,
        aws_profile=args.aws_profile,
    )
    _LOG.info("Files found at %s:\n%s", args.src_dir, "\n".join(files))
    for file in files:
        full_path = os.path.join(args.src_dir, file)
        _LOG.debug("Converting %s...", full_path)


        df = pd.read_parquet(full_path)
        df.to_csv(full_path)

        # # Set datetime index.
        # reindexed_df = imvcdttrut.reindex_on_datetime(df, args.datetime_col)
        # # Add date partition columns to the dataframe.
        # df, partition_cols = hparque.add_date_partition_columns(
        #     reindexed_df, "by_year_month"
        # )
        # hparque.to_partitioned_parquet(
        #     df,
        #     [args.asset_col] + partition_cols,
        #     args.dst_dir,
        #     aws_profile=args.aws_profile,
        # )
    #hparque.list_and_merge_pq_files(args.dst_dir, aws_profile="ck")


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--src_dir",
        action="store",
        type=str,
        required=True,
        help="Dir with input Parquet files to convert to CSV format",
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        type=str,
        required=True,
        help="Destination dir where to save converted CSV files",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Skip files that have already been converted",
    )
    parser.add_argument(
        "--aws_profile",
        action="store",
        required=True,
        type=str,
        help="The AWS profile to use for .aws/credentials or for env vars",
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser

def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _run(args)

if __name__ == "__main__":
    _main(_parse())