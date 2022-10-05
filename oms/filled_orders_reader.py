"""
Import as:

import oms.filled_orders_reader as ofiorrea
"""
import logging
import os
import re

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import oms.hsecrets as homssec
import helpers.hio as hio
from typing import Union, Dict, List, Any

_LOG = logging.getLogger(__name__)


class FilledOrdersReader:
    """
    Read data on filled orders conducted on a given account.

    The data is downloaded via `get_ccxt_fills` script. The reader deals
    with both JSON and CSV formats.
    """

    def __init__(
        self, root_dir: str, secret_identifier: homssec.SecretIdentifier
    ):
        """
        Constructor.

        :param root_dir: root data location, e.g. `/shared_data/filled_orders/`
        :param secret_identifier: AWS identifier for the account, i.e. `***REMOVED***`
        """
        self._root_dir = root_dir
        self._secret_identifier = secret_identifier

    def get_file_names_for_time_period(
        self, start_ts: pd.Timestamp, end_ts: pd.Timestamp, file_format: str
    ):
        """
        Get files that correspond to the given time period.

        Example of a file name:
        'fills_20221001-000000_20221004-000000_***REMOVED***.csv.gz'

        :param start_ts: start of time period
        :param end_ts: end of time period
        :param file_format: 'json' or 'csv'
        """
        hdbg.dassert_in(file_format, ["csv", "json"])
        root_dir = os.path.join(self._root_dir, file_format)
        # Change file format to allow reading gzipped csv files.
        if file_format == "csv":
            file_format = "csv.gz"
        # Get files for the given time range.
        files = os.listdir(root_dir)
        # Get files for the target account, e.g. only for `***REMOVED***`.
        files = [f for f in files if str(self._secret_identifier) in f]
        # Search for filename date patterns, e.g.
        #  '20221001-000000_20221004-000000'.
        pattern = re.compile(r"(\d+-\d+)_(\d+-\d+)")
        date_ranges_as_str = []
        date_ranges_as_timestamps = []
        # Select all found time ranges from files.
        for file in files:
            date_range = re.findall(pattern, file)
            date_ranges_as_str.extend(date_range)

        # Get files that fit into the given time range.
        #
        # Get start timestamp closest to start_ts.
        start_ts_in_files = [pd.Timestamp(date_range[0], tz="UTC") for date_range in date_ranges]
        if min(start_ts_in_files) > start_ts:
            start_ts_from_file_name = min(start_ts_in_files)
            _LOG.warning("min_ts is outside available data. Available min_ts=%s", start_ts_from_file_name)
        else:
            start_ts_from_file_name = max([ts for ts in start_ts_in_files if ts <= start_ts])

        # Get end timestamp closest to end_ts.
        end_ts_in_files = [pd.Timestamp(date_range[1], tz="UTC") for date_range in date_ranges]
        if max(end_ts_in_files) < end_ts:
            end_ts_from_file_name = max(end_ts_in_files)
            _LOG.warning("end_ts is outside available data. Available end_ts=%s", end_ts_from_file_name)
        else:
            end_ts_from_file_name = min([ts for ts in end_ts_in_files if ts >= end_ts])
        # Select files closest to the given time boundaries.
        target_paths = []
        for date_range in date_ranges:
            if (
                date_range[0] >= start_ts_from_file_name
                and date_range[1] <= end_ts_from_file_name
            ):
                path = os.path.join(
                    root_dir,
                    f"fills_{date_range[0]}_{date_range[1]}_{self._secret_identifier}.{file_format}",
                )
                target_paths.append(path)
        return target_paths

    # TODO(Danya): Update to use with JSON.
    def read_filled_orders(
        self,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        file_format: str
    ) -> Union[List[Dict[str, Any]], pd.DataFrame]:
        """
        Read a .csv file for filled trades.

        An example of output data: same as `FilledTradesReader.convert_fills_json_to_dataframe`
        """
        hdbg.dassert_in(file_format, ["json", "csv"])
        # Assert that passed timestamps have timezones.
        hdateti.dassert_has_tz(start_ts)
        hdateti.dassert_has_tz(end_ts)
        # Get files corresponding to the given time period.
        file_names = self.get_file_names_for_time_period(start_ts, end_ts, file_format)
        filled_trades_data = []
        if file_format == "json":
            # Load JSON as a list of dicts.
            for file_name in file_names:
                data = hio.from_json(file_name)
                filled_trades_data.extend(data)
        elif file_format == "csv":
            # Load CSV files as a dataframe.
            for file_name in file_names:
                df = pd.read_csv(
                    file_name, index="timestamp", parse_dates=["timestamp"]
                )
                filled_trades_data.append(df)
            filled_trades_data = pd.concat(filled_trades_data)
            # Filter data outside the given time period.
            filled_trades_data = filled_trades_data.loc[
                (filled_trades_data["timestamp"] >= start_ts)
                & (filled_trades_data["timestamp"] <= end_ts)
            ]
            # Set dtypes.
            filled_trades_data = filled_trades_data.astype(
                {
                    "id": int,
                    "order": int,
                    "price": float,
                    "amount": float,
                    "cost": float,
                    "fees": float,
                    "realized_pnl": float,
                }
            )
        return filled_trades_data
