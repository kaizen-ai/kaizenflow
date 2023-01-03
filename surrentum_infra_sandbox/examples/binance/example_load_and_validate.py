#!/usr/bin/env python
"""
Example implementation of abstract classes for ETL and QA pipeline.

Load and validate data within a specified time period from a CSV file.

# Load OHLCV data for binance:
> example_load_and_validate.py \
    --start_timestamp '2022-10-20 12:00:00+00:00' \
    --end_timestamp '2022-10-21 12:00:00+00:00' \
    --source_dir '.' \
    --dataset_signature 'bulk.manual.download_1min.csv.ohlcv.spot.v7.binance.binance.v1_0_0'
"""
import argparse
import logging
import os
from datetime import timedelta
from typing import Any, List

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import surrentum_infra_sandbox.client as sinsacli
import surrentum_infra_sandbox.validate as sinsaval

_LOG = logging.getLogger(__name__)


# #############################################################################
# CsvClient
# #############################################################################


class CsvClient(sinsacli.DataClient):
    """
    Class for loading CSV data from local filesystem into main memory.
    """

    def __init__(self, source_dir: str) -> None:
        """
        Constructor.

        :param source_dir: path to source data from.
        """
        self.source_dir = source_dir

    def load(
        self,
        dataset_signature: str,
        start_timestamp=None,
        end_timestamp=None,
        **kwargs: Any,
    ) -> Any:
        """
        Load CSV data specified by a unique signature from a desired source
        directory for a specified time period.

        The method assumes data having a 'timestamp' column.

        :param dataset_signature: signature of the dataset to load
        :param start_timestamp: beginning of the time period to load (context differs based
         on data type). If None, start with the earliest saved data.
        :param end_timestamp: end of the time period to load (context differs based
         on data type). If None, download up to the latest saved data.
        :return: loaded data
        """
        # TODO(Juraj): rewrite using dataset_schema_utils.
        dataset_signature += ".csv"
        source_path = os.path.join(self.source_dir, dataset_signature)
        data = pd.read_csv(source_path)
        if start_timestamp:
            hdateti.dassert_has_tz(start_timestamp)
            start_timestamp_as_unix = hdateti.convert_timestamp_to_unix_epoch(
                start_timestamp
            )
            data = data[data.timestamp >= start_timestamp_as_unix]
        if end_timestamp:
            hdateti.dassert_has_tz(end_timestamp)
            end_timestamp_as_unix = hdateti.convert_timestamp_to_unix_epoch(
                end_timestamp
            )
            data = data[data.timestamp < end_timestamp_as_unix]
        return data


# #############################################################################
# Example QA checks and validator implementation
# #############################################################################


class EmptyDatasetCheck(sinsaval.QaCheck):
    def check(self, dataframes: List[pd.DataFrame], *args: Any) -> bool:
        """
        Assert a DataFrame is not empty.
        """
        hdbg.dassert_eq(len(dataframes), 1)
        is_empty = dataframes[0].empty
        self._status = "FAILED: Dataset is empty" if is_empty else "PASSED"
        return not is_empty


class GapsInTimestampCheck(sinsaval.QaCheck):
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
        Assert a DataFrame does not have gaps in its timestamp column.
        """
        hdbg.dassert_eq(len(datasets), 1)
        data = datasets[0]
        # We check for gaps in the timestamp for each symbol individually.
        df_gaps = []
        for symbol in data["currency_pair"].unique():
            data_current = data[data["currency_pair"] == symbol]
            df_gaps_current = hpandas.find_gaps_in_time_series(
                data_current["timestamp"],
                self.start_timestamp,
                self.end_timestamp,
                self.freq,
            )
            if not df_gaps_current.empty:
                df_gaps.append((symbol, df_gaps_current))

        self._status = (
            f"FAILED: Dataset has timestamp gaps: \n {df_gaps}"
            if df_gaps != []
            else "PASSED"
        )
        return df_gaps == []


class SingleDatasetValidator(sinsaval.DatasetValidator):
    def run_all_checks(self, datasets: List, logger: logging.Logger) -> None:
        error_msgs: List[str] = []
        hdbg.dassert_eq(len(datasets), 1)
        _LOG.info("Running all QA checks:")
        for qa_check in self.qa_checks:
            if qa_check.check(datasets):
                logger.info(qa_check.get_status())
            else:
                error_msgs.append(qa_check.get_status())
        if error_msgs:
            error_msg = "\n".join(error_msgs)
            hdbg.dfatal(error_msg)


# #############################################################################
# Example script setup
# #############################################################################


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(use_exec_path=True)
    # Convert timestamps.
    start_timestamp = pd.Timestamp(args.start_timestamp)
    end_timestamp = pd.Timestamp(args.end_timestamp)
    csv_client = CsvClient(args.source_dir)
    data = csv_client.load(args.dataset_signature, start_timestamp, end_timestamp)
    empty_dataset_check = EmptyDatasetCheck()
    # Conforming to the (a, b] interval convention, remove 1 minute
    #  from the end_timestamp.
    gaps_in_timestamp_check = GapsInTimestampCheck(
        start_timestamp, end_timestamp - timedelta(minutes=1)
    )
    dataset_validator = SingleDatasetValidator(
        [empty_dataset_check, gaps_in_timestamp_check]
    )
    dataset_validator.run_all_checks([data], _LOG)


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
