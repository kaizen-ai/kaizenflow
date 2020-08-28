#!/usr/bin/env python

r"""Converts Kibot data on S3 from .csv.gz to Parquet.

# Process only a specific dataset:
> convert_csv_to_pq.py --dataset all_stocks_1min

# Process several datasets:
> convert_csv_to_pq.py --dataset all_stocks_1min --dataset all_stocks_daily

# Start from scratch and process all datasets:
> convert_csv_to_pq.py --delete_s3_dir

# Debug
> convert_csv_to_pq.py --serial -v DEBUG
"""

import argparse
import logging
import os
from typing import Any, Callable, List, Optional

import joblib
import pandas as pd
import tqdm

import helpers.csv as csv
import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.s3 as hs3
import helpers.system_interaction as si
import vendors2.kibot.data.config as config

_LOG = logging.getLogger(__name__)

_JOBLIB_NUM_CPUS = 10
_JOBLIB_VERBOSITY = 1


# #############################################################################


# TODO(gp): Call the column datetime_ET suffix.
def _normalize_1_min(df: pd.DataFrame) -> pd.DataFrame:
    """Convert a df with 1 min Kibot data into our internal format.

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
        df.columns = "datetime open high low close vol".split()
        df.set_index("datetime", drop=True, inplace=True)
    else:
        df.columns = df.columns.astype(str)
        _LOG.warning("The dataframe has only one column:\n%s", df)
    dbg.dassert(df.index.is_monotonic_increasing)
    dbg.dassert(df.index.is_unique)
    return df


def _normalize_daily(df: pd.DataFrame) -> pd.DataFrame:
    """Convert a df with daily Kibot data into our internal format.

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
    """Choose a normalizer function based on a dataset name.

    :param dataset: dataset name
    """
    ret = None
    if dataset.endswith("1min"):
        ret = _normalize_1_min
    elif dataset.endswith("daily"):
        ret = _normalize_daily
    elif dataset.endswith("tick"):
        dbg.dfatal("Support for dataset '%s' not implemented yet")
    else:
        dbg.dfatal("Unexpected dataset '%s'", dataset)
    return ret


# #############################################################################


def _convert_kibot_csv_gz_to_pq(
    dataset: str,
    symbol: str,
    dataset_aws_csv_gz_dir: str,
    dataset_source_dir: str,
    dataset_converted_dir: str,
    dataset_aws_pq_dir: str,
    skip_if_exists: bool,
) -> bool:
    """Convert a Kibot dataset for symbol.

    This requires to:
    - download a single .csv.gz payload from S3 into source directory,
    - convert it into .pq format and store into converted directory
    - upload back to S3

    :param symbol: symbol to process
    :param dataset_aws_csv_gz_dir: S3 dataset directory with .csv.gz files
    :param dataset_source_dir: local directory to store .csv.gz files
    :param dataset_converted_dir: local directory to store .pq files
    :param dataset_aws_pq_dir: S3 dataset directory with .pq files
    :param skip_if_exists: do not process if it exists
    :return: True if it was processed
    """

    _LOG.debug("Converting '%s' symbol for the dataset '%s'", symbol, dataset)
    # Prepare the PQ file.
    pq_filename = "%s.pq" % symbol
    pq_s3_filepath = os.path.join(dataset_aws_pq_dir, pq_filename)
    pq_filepath = os.path.join(dataset_converted_dir, pq_filename)
    # Check if S3 file exists.
    if skip_if_exists:
        exists = hs3.exists(pq_s3_filepath)
        if exists:
            _LOG.info("'%s' already exists: skipping", pq_s3_filepath)
            return False
    # Prepare the CSV file.
    csv_gz_filename = "%s.csv.gz" % symbol
    csv_s3_filepath = os.path.join(dataset_aws_csv_gz_dir, csv_gz_filename)
    csv_filepath = os.path.join(dataset_source_dir, csv_gz_filename)
    #
    _LOG.debug("Downloading s3 file %s into %s", csv_s3_filepath, csv_filepath)
    cmd = "aws s3 cp %s %s" % (csv_s3_filepath, csv_filepath)
    si.system(cmd)
    #
    _LOG.debug("Converting %s file into %s", csv_filepath, pq_filepath)
    normalizer = _get_normalizer(dataset)
    compression = "gzip"
    csv.convert_csv_to_pq(csv_filepath, pq_filepath, normalizer, compression=compression)
    #
    _LOG.debug("Uploading %s file into %s", pq_filepath, pq_s3_filepath)
    cmd = "aws s3 cp %s %s" % (pq_filepath, pq_s3_filepath)
    si.system(cmd)
    return True


def _compare_kibot_csv_gz_to_pq(
    dataset: str,
    symbol: str,
    dataset_aws_csv_gz_dir: str,
    dataset_source_dir: str,
    dataset_converted_dir: str,
    dataset_aws_pq_dir: str,
) -> None:
    """Ensure that the converted data matches the original data.

    This requires to:
    - download a single .csv.gz payload from S3 into source directory
    - convert it into .pq format and store it into converted directory
    - upload back to S3.

    :param symbol: symbol to process
    :param dataset_aws_csv_gz_dir: S3 dataset directory with .csv.gz files
    :param dataset_source_dir: local directory to store .csv.gz files
    :param dataset_converted_dir: local directory to store .pq files
    :param dataset_aws_pq_dir: S3 dataset directory with .pq files
    """
    _LOG.debug("Checking '%s' symbol for the dataset '%s'", symbol, dataset)
    #
    csv_gz_filename = "%s.csv.gz" % symbol
    csv_s3_filepath = os.path.join(dataset_aws_csv_gz_dir, csv_gz_filename)
    csv_filepath = os.path.join(dataset_source_dir, csv_gz_filename)
    # Make sure that the `.csv.gz` file is downloaded.
    if os.path.exists(csv_filepath):
        _LOG.debug("'%s' already exists", csv_filepath)
    else:
        _LOG.debug(
            "Downloading s3 file '%s' into '%s'", csv_s3_filepath, csv_filepath
        )
        cmd = "aws s3 cp %s %s" % (csv_s3_filepath, csv_filepath)
        si.system(cmd)
    #
    pq_filename = "%s.pq" % symbol
    pq_s3_filepath = os.path.join(dataset_aws_pq_dir, pq_filename)
    pq_filepath = os.path.join(dataset_converted_dir, pq_filename)
    # Make sure that the `.pq` file is downloaded.
    if os.path.exists(pq_filepath):
        _LOG.debug("'%s' already exists", pq_filepath)
    else:
        _LOG.debug("Downloading s3 file '%s' into '%s'", pq_s3_filepath, pq_filepath)
        cmd = "aws s3 cp %s %s" % (pq_s3_filepath, pq_filepath)
        si.system(cmd)
    # Read the PQ file.
    pq_df = pd.read_parquet(pq_filepath)
    csv_df = pd.read_csv(csv_filepath, header=None)
    normalizer = _get_normalizer(dataset)
    if normalizer is not None:
        csv_df = normalizer(csv_df)
    # If there is an issue assert and report error.
    if not csv_df.equals(pq_df):
        csv_df.to_csv("csv_df.csv")
        pq_df.to_csv("pq_df.csv")
        raise ValueError("The dataframes are different: saved in files")


# #############################################################################


def _get_symbols_to_process(
    dataset_aws_csv_gz_dir: str
) -> List[str]:
    """Get a list of symbols that need a .pq file on S3.

    :param dataset_aws_csv_gz_dir: S3 dataset directory with .csv.gz files
    :return: list of symbols
    """

    def _extract_filename_without_extension(file_path: str) -> str:
        """Return only basename of the path without the .csv.gz or .pq
        extensions.

        :param file_path: a full path of a file
        :return: file name without extension
        """
        filename = os.path.basename(file_path)
        filename = filename.replace(".csv.gz", "")
        filename = filename.replace(".pq", "")
        return filename

    # List all existing csv gz files on S3.
    csv_gz_s3_file_paths = hs3.listdir(dataset_aws_csv_gz_dir)
    # Get list of symbols to convert.
    symbols = list(
        map(_extract_filename_without_extension, csv_gz_s3_file_paths)
    )
    dbg.dassert_no_duplicates(symbols)
    symbols = sorted(list(set(symbols)))
    return symbols


def _process_over_dataset(
    fn: Callable, symbols: List[str], serial: bool, **kwargs: Any
) -> None:
    """Process in parallel each symbol in the list.

    :param fn: a procedure to be run for each symbol
    :param symbols: list of symbols to run fn over
    :param serial: whether to run sequentially
    :param kwargs: other arguments to pass to fn
    """
    tqdm_ = tqdm.tqdm(symbols, desc="Process symbol", total=len(symbols))
    if serial:
        for symbol in tqdm_:
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
        choices=config.DATASETS,
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
        "--skip_compare", action="store_true", help="Skip compare step",
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
    dbg.shutup_chatty_modules()
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
    aws_csv_dir = os.path.join("s3://", config.S3_PREFIX)
    _LOG.info("aws_csv_dir=%s", aws_csv_dir)
    aws_pq_dir = os.path.join("s3://", config.S3_PREFIX, "pq")
    _LOG.info("aws_pq_dir=%s", aws_pq_dir)
    # Clean up S3 if needed.
    if args.delete_s3_dir:
        assert 0, "Very dangerous: are you sure?"
        _LOG.warning("Deleting s3 file %s", aws_pq_dir)
        cmd = "aws s3 rm --recursive %s" % aws_pq_dir
        si.system(cmd)
    #
    datasets_to_proceed = args.dataset or config.DATASETS
    _LOG.info(
        "datasets=%d %s", len(datasets_to_proceed), ", ".join(datasets_to_proceed)
    )
    # Process a dataset.
    for dataset in tqdm.tqdm(datasets_to_proceed, desc="Process dataset"):
        # Create dataset dirs.
        dataset_source_dir = os.path.join(source_dir, dataset)
        io_.create_dir(dataset_source_dir, incremental=incremental)
        dataset_converted_dir = os.path.join(converted_dir, dataset)
        io_.create_dir(dataset_converted_dir, incremental=incremental)
        # Define S3 dirs.
        dataset_aws_csv_gz_dir = os.path.join(aws_csv_dir, dataset)
        dataset_aws_pq_dir = os.path.join(aws_pq_dir, dataset)
        # Get the symbols.
        _LOG.info(
            "# Look for list of symbols to process for the dataset '%s'",
            dataset,
        )
        if False:
            symbols = _get_symbols_to_process(dataset_aws_csv_gz_dir)
        else:
            symbols = ["AAPL"]
        _LOG.info("Found %d symbols", len(symbols))
        #
        _LOG.debug("# Convert files for the dataset '%s'", dataset)
        _process_over_dataset(
            _convert_kibot_csv_gz_to_pq,
            symbols,
            args.serial,
            dataset=dataset,
            dataset_aws_csv_gz_dir=dataset_aws_csv_gz_dir,
            dataset_source_dir=dataset_source_dir,
            dataset_converted_dir=dataset_converted_dir,
            dataset_aws_pq_dir=dataset_aws_pq_dir,
            skip_if_exists=not args.no_skip_if_exists,
        )
        #
        if not args.skip_compare:
            _LOG.debug("# Checking files for the dataset '%s'", dataset)
            _process_over_dataset(
                _compare_kibot_csv_gz_to_pq,
                symbols,
                args.serial,
                dataset=dataset,
                dataset_aws_csv_gz_dir=dataset_aws_csv_gz_dir,
                dataset_source_dir=dataset_source_dir,
                dataset_converted_dir=dataset_converted_dir,
                dataset_aws_pq_dir=dataset_aws_pq_dir,
            )
        else:
            _LOG.warning("Skipping compare as per user request")


if __name__ == "__main__":
    _main(_parse())
