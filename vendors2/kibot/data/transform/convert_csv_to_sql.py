#!/usr/bin/env python
r"""
Converts Kibot data on S3 from .csv.gz to SQL and inserts it into DB.
"""

import argparse
import logging
import os
from typing import Any, Callable, List, Optional

import joblib
import pandas as pd
import tqdm

import helpers.dbg as dbg
import helpers.parser as hparse
import helpers.s3 as hs3
import vendors2.kibot.data.config as vkdcon
import vendors2.kibot.data.load as vkdloa
import vendors2.kibot.data.types as vkdtyp

_LOG = logging.getLogger(__name__)

_JOBLIB_NUM_CPUS = 10
_JOBLIB_VERBOSITY = 1

# #############################################################################


def _insert_into_db(df: pd.DataFrame) -> None:
    # TODO(vr): This will receive a DataFrame and will execute an INSERT query to PSQL
    print(df.head())


def _convert_kibot_csv_gz_to_sql(
    symbol: str,
    downloader: vkdloa.KibotDataLoader,
    asset_class: vkdtyp.AssetClass,
    frequency: vkdtyp.Frequency,
    contract_type: Optional[vkdtyp.ContractType] = None,
    unadjusted: Optional[bool] = None,
) -> bool:
    """
    Convert a Kibot dataset for a symbol.

    :param symbol: symbol to process
    :return: True if it was processed
    """
    df = downloader.read_data(
        symbol,
        asset_class=asset_class,
        frequency=frequency,
        contract_type=contract_type,
        unadjusted=unadjusted,
    )
    _insert_into_db(df=df)
    return True


def _get_symbols_to_process(aws_csv_gz_dir: str) -> List[str]:
    """
    Get a list of symbols that need a .pq file on S3.

    :param aws_csv_gz_dir: S3 dataset directory with .csv.gz files
    :return: list of symbols
    """

    def _extract_filename_without_extension(file_path: str) -> str:
        """
        Return only basename of the path without the .csv.gz or .pq extensions.

        :param file_path: a full path of a file
        :return: file name without extension
        """
        filename = os.path.basename(file_path)
        filename = filename.replace(".csv.gz", "")
        return filename

    # List all existing csv gz files on S3.
    csv_gz_s3_file_paths = hs3.listdir(aws_csv_gz_dir)
    # Get list of symbols to convert.
    symbols = list(map(_extract_filename_without_extension, csv_gz_s3_file_paths))
    dbg.dassert_no_duplicates(symbols)
    symbols = sorted(list(set(symbols)))
    return symbols


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
        "--serial",
        # TODO(vr): Change this back.
        # action="store_true",
        action="store_false",
        help="Download data serially",
    )
    parser.add_argument(
        "--max_num_assets",
        action="store",
        type=int,
        # TODO(vr): Change this back.
        # default=None,
        default=10,
        help="Maximum number of assets to copy (for debug)",
    )
    hparse.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    dbg.shutup_chatty_modules()
    # Create Kibot Downloader class.
    downloader = vkdloa.KibotDataLoader()
    _LOG.info("Created class")
    # Define data directory.
    aws_csv_gz_dir = os.path.join(
        vkdcon.S3_PREFIX, "All_Futures_Continuous_Contracts_1min"
    )
    # Get the symbols.
    symbols = _get_symbols_to_process(aws_csv_gz_dir)
    # symbols = ["AAPL"]
    if args.max_num_assets is not None:
        dbg.dassert_lte(1, args.max_num_assets)
        symbols = symbols[: args.max_num_assets]
    _LOG.info("Found %d symbols", len(symbols))
    #
    _process_over_dataset(
        _convert_kibot_csv_gz_to_sql,
        symbols,
        args.serial,
        downloader=downloader,
        asset_class=vkdtyp.AssetClass.Futures,
        frequency=vkdtyp.Frequency.Minutely,
        contract_type=vkdtyp.ContractType.Continuous,
    )
    #


if __name__ == "__main__":
    _main(_parse())
