#!/usr/bin/env python

r"""
Convert Kibot data from csv.gz to Parquet.

The data is located in `kibot` directory on S3 and is separated into
several subdirectories.
The files in the following subdirectories:
- `All_Futures_Contracts_1min`
- `All_Futures_Continuous_Contracts_1min`
- `All_Futures_Continuous_Contracts_daily`
are converted to Parquet and saved to 'kibot/pq` in corresponding
subdirectories.

Usage example:
> python vendors/kibot/convert_kibot_to_pq.py -v DEBUG

After the conversion the data layout looks like:
> aws s3 ls default00-bucket/kibot/
                           PRE All_Futures_Continuous_Contracts_1min/
                           PRE All_Futures_Continuous_Contracts_daily/
                           PRE All_Futures_Continuous_Contracts_tick/
                           PRE All_Futures_Contracts_1min/
                           PRE All_Futures_Contracts_daily/
                           PRE metadata/
                           PRE pq/

> aws s3 ls default00-bucket/kibot/pq/
                           PRE All_Futures_Continuous_Contracts_1min/
                           PRE All_Futures_Continuous_Contracts_daily/
                           PRE All_Futures_Contracts_1min/
                           PRE All_Futures_Contracts_daily/
"""

import argparse
import logging
import os
from typing import Callable, Optional

import pandas as pd
import tqdm

import helpers.csv as csv
import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.s3 as hs3
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# S3 bucket to save the data.
_S3_URI = "external-p1/kibot"

_DATASETS = [
    "all_stocks_1min",
    "all_stocks_unadjusted_1min",
    "all_stocks_daily",
    "all_stocks_unadjusted_daily",
    "all_etfs_1min",
    "all_etfs_unadjusted_1min",
    "all_etfs_daily",
    "all_etfs_unadjusted_daily",
    "all_forex_pairs_1min",
    "all_forex_pairs_daily",
    "all_futures_contracts_1min",
    "all_futures_contracts_daily",
    "all_futures_continuous_contracts_tick",
    "all_futures_continuous_contracts_1min",
    "all_futures_continuous_contracts_daily",
]


# #############################################################################

# TODO(vr): Implement this one.
def _normalize_tick(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert a df with tick data from kibot.

    :param df: kibot raw dataframe as it is in .csv.gz files
    :return: a dataframe
    """


# TODO(gp): Call the column datetime_ET suffix.
def _normalize_1_min(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert a df with 1 min Kibot data into our internal format.

    - Combine the first two columns into a datetime index
    - Add column names
    - Check for monotonic index

    :param df: kibot raw dataframe as it is in .csv.gz files
    :return: a dataframe with `datetime` index and `open`, `high`,
        `low`, `close`, `vol` columns. If the input dataframe
        has only one column, the column name will be transformed to
        string format.
    """
    # There are cases in which the dataframes consist of only one column,
    # with the first row containing a `405 Data Not Found` string, and
    # the second one containing `No data found for the specified period
    # for BTSQ14.`
    if df.shape[1] > 1:
        # According to Kibot the columns are:
        #   Date,Time,Open,High,Low,Close,Volume
        # Convert date and time into a datetime.
        df[0] = pd.to_datetime(df[0] + " " + df[1], format="%m/%d/%Y %H:%M")
        df.drop(columns=[1], inplace=True)
        # Rename columns.
        columns = "datetime open high low close vol".split()
        df.columns = columns
        df.set_index("datetime", drop=True, inplace=True)
        _LOG.debug("Add columns")
    else:
        df.columns = df.columns.astype(str)
        _LOG.warning("The dataframe has only one column: %s", df)
    dbg.dassert(df.index.is_monotonic_increasing)
    dbg.dassert(df.index.is_unique)
    return df


def _normalize_daily(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert a df with daily Kibot data into our internal format.

    - Convert the first column to datetime and set is as index
    - Add column names
    - Check for monotonic index

    :param df: kibot raw dataframe as it is in .csv.gz files
    :return: a dataframe with `datetime` index and `open`, `high`,
        `low`, `close`, `vol` columns.
    """
    # Convert date and time into a datetime.
    df[0] = pd.to_datetime(df[0], format="%m/%d/%Y")
    # Rename columns.
    df.columns = "datetime open high low close vol".split()
    df.set_index("datetime", drop=True, inplace=True)
    # TODO(gp): Turn date into datetime using EOD timestamp. Check on Kibot.
    dbg.dassert(df.index.is_monotonic_increasing)
    dbg.dassert(df.index.is_unique)
    return df


def _get_normalizer(dataset: str) -> Optional[Callable]:
    """
    Chose a normalizer function based on a dataset name.

    :param dataset: dataset name
    :return: `_normalize_1_min`, `_normalize_daily` or None
    """
    if dataset in [
        "all_stocks_1min",
        "all_stocks_unadjusted_1min",
        "all_etfs_1min",
        "all_etfs_unadjusted_1min",
        "all_forex_pairs_1min",
        "all_futures_contracts_1min",
        "all_futures_continuous_contracts_1min",
    ]:
        # 1 minute data.
        return _normalize_1_min
    if dataset in [
        "all_stocks_daily",
        "all_stocks_unadjusted_daily",
        "all_etfs_daily",
        "all_etfs_unadjusted_daily",
        "all_forex_pairs_daily",
        "all_futures_contracts_daily",
        "all_futures_continuous_contracts_daily",
    ]:
        # Daily data.
        return _normalize_daily
    if dataset in ["all_futures_continuous_contracts_tick"]:
        # Tick data.
        return _normalize_tick
    message = "Unexpected dataset %s" % dataset
    dbg.dfatal(message)


def _download_kibot_csv_gz_dataset(dataset: str, source_dataset_dir: str) -> None:
    """
    Download all .csv.gz files from S3 and store locally for processing.

    :param dataset: dataset name
    :param source_dataset_dir: tmp source directory to store csv files
    """
    csv_dataset_s3_path = os.path.join("s3://", _S3_URI, dataset)
    csv_filenames = hs3.listdir(csv_dataset_s3_path)
    for csv_filename in csv_filenames:
        csv_s3_filepath = os.path.join(csv_dataset_s3_path, csv_filename)
        csv_filepath = os.path.join(source_dataset_dir, csv_filename)
        # Copy from s3.
        cmd = "aws s3 cp %s %s" % (csv_s3_filepath, csv_filepath)
        si.system(cmd)


def _convert_kibot_csv_gz_to_pq(
    dataset: str, source_dataset_dir: str, converted_dataset_dir: str
) -> None:
    """
    Convert the files in the following subdirs of `kibot` dir on S3 to Parquet.
    Save them to `kibot/pq` dir on S3.

    :param dataset: dataset name
    :param source_dataset_dir: tmp directory to store csv files
    :param converted_dataset_dir: tmp directory to store pq files
    """
    normalizer = _get_normalizer(dataset)
    csv.convert_csv_dir_to_pq_dir(
        source_dataset_dir, converted_dataset_dir, header=None, normalizer=normalizer
    )
    _LOG.info(
        "Converted the files in %s directory and saved them to %s.",
        source_dataset_dir,
        converted_dataset_dir,
    )


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--tmp_dir",
        type=str,
        nargs="?",
        help="Directory to store temporary data",
        default="tmp.kibot_converter",
    )
    parser.add_argument(
        "--dataset",
        type=str,
        help="Download a specific dataset (or all datasets if omitted)",
        choices=_DATASETS,
        action="append",
        default=None,
    )
    parser.add_argument(
        "--no_incremental",
        action="store_true",
        help="Clean the local directories",
    )
    parser.add_argument(
        "--no_skip_if_exists",
        action="store_true",
        help="Do not skip if it exists on S3",
    )
    parser.add_argument(
        "--no_clean_up_artifacts",
        action="store_true",
        help="Do not clean artifacts",
    )
    parser.add_argument(
        "--delete_s3_dir",
        action="store_true",
        help="Delete the S3 dir before starting uploading (dangerous)",
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Create dirs.
    incremental = not args.no_incremental
    io_.create_dir(args.tmp_dir, incremental=incremental)
    #
    source_dir_name = "source_data"
    source_dir = os.path.join(args.tmp_dir, source_dir_name)
    io_.create_dir(source_dir, incremental=incremental)
    #
    converted_dir_name = "converted_data"
    converted_dir = os.path.join(args.tmp_dir, converted_dir_name)
    io_.create_dir(converted_dir, incremental=incremental)
    aws_dir = os.path.join("s3://", _S3_URI, "pq")
    if args.delete_s3_dir:
        assert 0, "Very dangerous: are you sure"
        _LOG.warning("Deleting s3 file %s", aws_dir)
        cmd = "aws s3 rm --recursive %s" % aws_dir
        si.system(cmd)
    datasets_to_proceed = args.dataset or _DATASETS
    # Process a dataset.
    for dataset in tqdm.tqdm(datasets_to_proceed, desc="dataset"):
        _LOG.debug("Convert files for the following dataset: %s", dataset)
        # Create dataset dirs.
        dataset_source_dir = os.path.join(source_dir, dataset)
        io_.create_dir(dataset_source_dir, incremental=incremental)
        #
        dataset_converted_dir = os.path.join(converted_dir, dataset)
        io_.create_dir(dataset_converted_dir, incremental=incremental)
        _download_kibot_csv_gz_dataset(dataset, dataset_source_dir)
        _convert_kibot_csv_gz_to_pq(
            dataset, dataset_source_dir, dataset_converted_dir
        )
        # TODO(vr): Implement upload to s3 aws_dir.


if __name__ == "__main__":
    _main(_parse())
