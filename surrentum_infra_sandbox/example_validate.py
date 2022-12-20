#!/usr/bin/env python
"""
Example implementation of abstract classes for ETL and QA pipeline.

Load and validate data within a specified time period from a CSV file.

Use as:
# Load OHLCV data for binance:
> example_validate.py \
    --start_timestamp '2022-10-20 12:00:00+00:00' \
    --end_timestamp '2022-10-21 12:00:00+00:00' \
    --source_dir '.' \
    --dataset_signature 'bulk.manual.download_1min.csv.ohlcv.spot.v7.binance.binance.v1_0_0'
"""
import argparse
import logging
from typing import Any, List

import pandas as pd

import helpers.hdbg as hdbg
import surrentum_infra_sandbox.validate as sinsaval

_LOG = logging.getLogger(__name__)


class EmptyDatasetCheck(sinsaval.QaCheck):
    def check(self, dataframes: List[pd.DataFrame], *args: Any) -> bool:
        """
        Assert a DataFrame is not empty.
        """
        hdbg.dassert_eq(len(dataframes), 1)
        is_empty = dataframes[0].empty
        self._status = "FAILED: Dataset is empty" if is_empty else "PASSED"
        return is_empty


class GapsInDatasetIndexCheck(sinsaval.QaCheck):
    def __init__(
        self,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
        *,
        freq: str = "T",
    ) -> None:
        self.freq = freq
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp

    def check(self, datasets: List[pd.DataFrame], *args: Any) -> bool:
        """
        Assert a DataFrame does not have gaps in its DateTimeIndex.
        """
        hdbg.dassert_eq(len(datasets), 1)
        df_gaps = find_gaps_in_time_series(
            datasets[0].index, self.start_timestamp, self.end_timestamp, freq
        )
        self._status = (
            f"FAILED: Dataset has gaps: \n {df_gaps}"
            if not df_gaps.empty
            else "PASSED"
        )
        return df_gaps.empty


class SingleDatasetValidator(sinsaval.DatasetValidator):
    def run_all_checks(self, datasets: List, logger: logging.Logger) -> None:
        error_msgs: List[str] = []
        hdbg.dassert_eq(len(datasets), 1)
        for qa_check in self.qa_checks:
            if qa_check.check(datasets[0]):
                logger.info(check.get_status)
            else:
                error_msgs.append(check.get_status)
        if error_msgs:
            error_msg = "\n".join(error_msgs)
            hdbg.dfatal(error_msg)


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(use_exec_path=True)
    # Convert timestamps.
    start_timestamp = pd.Timestamp(args.start_timestamp)
    end_timestamp = pd.Timestamp(args.end_timestamp)
    csv_client = CsvClient(args.source_dir)
    data = csv_client.load(args.dataset_signature, start_timestamp, end_timestamp)
    empty_dataset_check = EmptyDatasetCheck()
    gaps_in_index_check = GapsInDatasetIndexCheck(start_timestamp, end_timestamp)
    dataset_validator = SingleDatasetValidator(
        [empty_dataset_check, gaps_in_index_check]
    )
    dataset_validator.run_all_checks([data])


def add_download_args(
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
        "--end_timestamp",
        action="store",
        required=True,
        type=str,
        help="End of the loaded period, e.g. 2022-02-10 10:00:00+00:00",
    )
    parser.add_argument(
        "--source_dir",
        action="store",
        required=True,
        type=str,
        help="Absolute path to the directory to source data from",
    )
    parser.add_argument(
        "--dataset_signature",
        action="store",
        required=True,
        type=str,
        help="Signature of the dataset (uniquely specifies a particular data set)",
    )
    return parser


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser = add_download_args(parser)
    return parser


if __name__ == "__main__":
    _main(_parse())
