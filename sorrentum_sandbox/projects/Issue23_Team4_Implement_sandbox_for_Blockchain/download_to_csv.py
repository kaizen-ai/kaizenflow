#!/usr/bin/env python
"""
Download OHLCV data from Binance and save it as CSV locally.
Use as:
> download_to_csv.py \
    --start_timestamp '2022-10-20 10:00:00+00:00' \
    --end_timestamp '2022-10-21 15:30:00+00:00' \
    --target_dir 'binance_data'
"""
import argparse
import logging
import os
from typing import Any

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hio as hio
import sorrentum_sandbox.common.download as sinsadow
import sorrentum_sandbox.examples.binance.download as sisebido
import sorrentum_sandbox.common.save as sinsasav

_LOG = logging.getLogger(__name__)


class CsvDataFrameSaver(sinsasav.DataSaver):
    """
    Class for saving pandas DataFrame as CSV to a local filesystem at desired
    location.
    """

    def __init__(self, target_dir: str) -> None:
        """
        Constructor.
        :param target_dir: path to save data to.
        """
        self.target_dir = target_dir

    def save(self, data: sinsadow.RawData, **kwargs: Any) -> None:
        """
        Save RawData storing a DataFrame to CSV.
        :param data: data to persists into CSV
        """
        hdbg.dassert_isinstance(data.get_data(), pd.DataFrame, "Only DataFrame is supported.")
        timestamp_str = pd.Timestamp.now().strftime("%Y-%m-%d_%H-%M-%S")
        signature = (
            f"bulk.manual.download_1min.csv.ohlcv.spot.v7.binance.binance.v1_0_0.{timestamp_str}"
        )
        signature += ".csv"
        hio.create_dir(self.target_dir, incremental=True)
        target_path = os.path.join(self.target_dir, signature)
        data.get_data().to_csv(target_path, index=False)

# #############################################################################


def _add_download_args(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    """
    Add the command line options for exchange download.
    """
    parser.add_argument(
        "--start_timestamp",
        required=True,
        action="store",
        type=str,
        help="Beginning of the loaded period, e.g. 2022-02-09 10:00:00+00:00",
    )
    parser.add_argument(
        "--time_span",
        action="store",
        required=True,
        type=str,
        help="End of the loaded period, e.g. 2022-02-10 10:00:00+00:00",
    )
    parser.add_argument(
        "--target_dir",
        action="store",
        required=True,
        type=str,
        help="Path to the target directory to store CSV data into",
    )

    parser.add_argument(
        "--api",
        action="store",
        default='https://api.blockchain.info/charts',
        type=str,
        help="Base URL for the API",
    )

    parser.add_argument(
        "--chart_name",
        action="store",
        default='transaction-fees-usd',
        required=True,
        type=str,
        help="Name of the chart to download",
    )


    
    return parser


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser = _add_download_args(parser)
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(
        verbosity=args.log_level,
        use_exec_path=True,
        # report_memory_usage=True
    )
    # Download data.
    # start_timestamp = pd.Timestamp(args.start_timestamp)
    # time_span = pd.Timedelta(args.time_span)
    downloader = sisebido.OhlcvRestApiDownloader(api=args.api, chart_name=args.chart_name)
    raw_data = downloader.download(args.start_timestamp,args.time_span)

    # Save data as CSV.
    saver = CsvDataFrameSaver(args.target_dir)
    saver.save(raw_data)



if __name__ == "__main__":
    _main(_parse())