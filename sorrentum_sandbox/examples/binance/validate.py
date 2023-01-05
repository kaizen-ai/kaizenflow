"""
QA pipeline for Binance.

Import as:

import sorrentum_sandbox.examples.binance.validate as sisebiva
"""

import logging
from typing import Any, List

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import sorrentum_sandbox.validate as sinsaval


def find_gaps_in_time_series(
    time_series: pd.Series,
    start_timestamp: pd.Timestamp,
    end_timestamp: pd.Timestamp,
    freq: str,
) -> pd.Series:
    """
    Find missing points on a time interval specified by [`start_timestamp`,
    `end_timestamp`], where point distribution is determined by `freq`.

    If the index of `time_series` is of a unix epoch format, it is automatically
    transformed to pd.Timestamp.

    :param time_series: time series to find gaps in
    :param start_timestamp: start of the time interval to check
    :param end_timestamp: end of the time interval to check
    :param freq: distance between two data points on the interval. Aliases
        correspond to `pandas.date_range` freq parameter, e.g., "S" for second,
        "T" for minute.
    :return: pd.Series representing missing points in the source time series
    """
    _time_series = time_series
    if str(time_series.dtype) in ["int32", "int64"]:
        _time_series = _time_series.map(hdateti.convert_unix_epoch_to_timestamp)
    correct_time_series = pd.date_range(
        start=start_timestamp, end=end_timestamp, freq=freq
    )
    return correct_time_series.difference(_time_series)


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
            df_gaps_current = find_gaps_in_time_series(
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
        logger.info("Running all QA checks:")
        for qa_check in self.qa_checks:
            if qa_check.check(datasets):
                logger.info(qa_check.get_status())
            else:
                error_msgs.append(qa_check.get_status())
        if error_msgs:
            error_msg = "\n".join(error_msgs)
            hdbg.dfatal(error_msg)