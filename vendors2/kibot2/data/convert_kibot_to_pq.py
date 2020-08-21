#!/usr/bin/env python

r"""
Convert Kibot data on S3 from .csv.gz to Parquet.

# Process only specific dataset:
> convert_kibot_to_pq.py --dataset all_stocks_1min

# Process several datasets:
> convert_kibot_to_pq.py --dataset all_stocks_1min --dataset all_stocks_daily

# Start from scratch and process all datasets:
> convert_kibot_to_pq.py --delete_s3_dir

# Debug
> convert_kibot_to_pq.py --serial -v DEBUG
"""

import argparse
import logging
import os
from typing import Optional, Callable

import joblib
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
        return None
    _LOG.error("Unexpected dataset %s", dataset)
    return None


def _extract_filename_without_extension(file_path: str) -> str:
    """
    Returns only basename of the path without the .csv.gz or .pq extensions.

    :param file_path: a full path of a file
    :return: file name without extension
    """
    filename = os.path.basename(file_path)
    filename = filename.replace(".csv.gz", "")
    filename = filename.replace(".pq", "")
    return filename


def _convert_kibot_csv_gz_to_pq(
    dataset: str,
    symbol: str,
    dataset_aws_csv_gz_dir: str,
    dataset_source_dir: str,
    dataset_converted_dir: str,
    dataset_aws_pq_dir: str,
) -> None:
    """
    Download single .csv.gz payload from S3 into source directory,
    convert it into .pq format, store into converted directory and upload back to S3.

    :param symbol: symbol to process
    :param dataset_aws_csv_gz_dir: S3 dataset directory with .csv.gz files
    :param dataset_source_dir: local directory to store .csv.gz files
    :param dataset_converted_dir: S3 dataset directory with .pq files
    :param dataset_aws_pq_dir: local directory to store .pq files
    :return:
    """
    normalizer = _get_normalizer(dataset)
    csv_gz_filename = "%s.csv.gz" % symbol
    pq_filename = "%s.pq" % symbol
    csv_s3_filepath = os.path.join(dataset_aws_csv_gz_dir, csv_gz_filename)
    csv_filepath = os.path.join(dataset_source_dir, csv_gz_filename)
    pq_s3_filepath = os.path.join(dataset_aws_pq_dir, pq_filename)
    pq_filepath = os.path.join(dataset_converted_dir, pq_filename)
    _LOG.debug("Downloading s3 file %s into %s", csv_s3_filepath, csv_filepath)
    cmd = "aws s3 cp %s %s" % (csv_s3_filepath, csv_filepath)
    si.system(cmd)
    _LOG.debug("Converting %s file into %s", csv_filepath, pq_filepath)
    csv.convert_csv_to_pq(csv_filepath, pq_filepath, normalizer)
    _LOG.debug("Uploading %s file into %s", pq_filepath, pq_s3_filepath)
    cmd = "aws s3 cp %s %s" % (pq_filepath, pq_s3_filepath)
    si.system(cmd)


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
        "--serial", action="store_true", help="Download data serially"
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
    # Define S3 dirs.
    aws_csv_dir = os.path.join("s3://", _S3_URI)
    aws_pq_dir = os.path.join("s3://", _S3_URI, "pq")
    if args.delete_s3_dir:
        assert 0, "Very dangerous: are you sure"
        _LOG.warning("Deleting s3 file %s", aws_pq_dir)
        cmd = "aws s3 rm --recursive %s" % aws_pq_dir
        si.system(cmd)
    datasets_to_proceed = args.dataset or _DATASETS
    # Process a dataset.
    for dataset in tqdm.tqdm(datasets_to_proceed, desc="dataset"):
        _LOG.debug("Convert files for the following dataset: %s", dataset)
        # Create dataset dirs.
        dataset_source_dir = os.path.join(source_dir, dataset)
        io_.create_dir(dataset_source_dir, incremental=incremental)
        dataset_converted_dir = os.path.join(converted_dir, dataset)
        io_.create_dir(dataset_converted_dir, incremental=incremental)
        # Define S3 dirs.
        dataset_aws_csv_gz_dir = os.path.join(aws_csv_dir, dataset)
        dataset_aws_pq_dir = os.path.join(aws_pq_dir, dataset)
        # List all existing csv gz files on S3.
        csv_gz_s3_file_paths = hs3.listdir(dataset_aws_csv_gz_dir)
        # Get exact list of symbols to convert.
        csv_gz_s3_symbols = list(
            map(_extract_filename_without_extension, csv_gz_s3_file_paths)
        )
        if args.no_skip_if_exists:
            s3_symbols = csv_gz_s3_symbols
        else:
            # List all existing pq files on S3.
            pq_s3_file_paths = hs3.listdir(dataset_aws_pq_dir)
            pq_s3_symbols = list(
                map(_extract_filename_without_extension, pq_s3_file_paths)
            )
            s3_symbols = list(
                set(csv_gz_s3_symbols).difference(set(pq_s3_symbols))
            )
        func = lambda symbol: _convert_kibot_csv_gz_to_pq(
            dataset,
            symbol,
            dataset_aws_csv_gz_dir,
            dataset_source_dir,
            dataset_converted_dir,
            dataset_aws_pq_dir,
        )
        tqdm_ = tqdm.tqdm(s3_symbols, total=len(s3_symbols))
        if not args.serial:
            joblib.Parallel(n_jobs=10, verbose=1)(
                joblib.delayed(func)(symbol) for symbol in tqdm_
            )
        else:
            for symbol in tqdm_:
                func(symbol)


if __name__ == "__main__":
    _main(_parse())
