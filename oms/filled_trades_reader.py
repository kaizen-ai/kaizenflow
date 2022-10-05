"""
Import as:

import oms.filled_trades_reader as ofitrrea
"""
import logging
import os
import re

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import oms.hsecrets as homssec

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
        'fills__***REMOVED***.csv.gz'

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
        # Search for filename date patterns, e.g.
        #  '20221001-000000_20221004-000000'.
        pattern = re.compile(r"(\d+-\d+)_(\d+-\d+)")
        date_ranges = []
        # Select all found time ranges from files.
        for file in files:
            date_range = re.findall(pattern, file)
            date_ranges.extend(date_range)
        # Get files that fit into the given time range.
        #
        # Get start timestamp closest to start_ts.
        start_ts_file_names = [
            drange[0]
            for drange in date_ranges
            if pd.Timestamp(drange[0]) <= start_ts
        ]
        start_ts_from_file_name = max(start_ts_file_names)
        # Get end timestamp closest to end_ts.
        end_ts_file_names = [
            drange[0]
            for drange in date_ranges
            if pd.Timestamp(drange[0]) >= end_ts
        ]
        end_ts_from_file_name = min(end_ts_file_names)
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
    def read_filled_orders_csv(
        self,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> pd.DataFrame:
        """
        Read a .csv file for filled trades.

        An example of output data: same as `FilledTradesReader.convert_fills_json_to_dataframe`
        """
        # Assert that passed timestamps have timezones.
        hdateti.dassert_has_tz(start_ts)
        hdateti.dassert_has_tz(end_ts)
        # Get files corresponding to the given time period.
        file_names = self.get_file_names_for_time_period(start_ts, end_ts, "csv")
        filled_trades_data = []
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
        # Set timestamp index.
        filled_trades_data = filled_trades_data.set_index("timestamp")
        return filled_trades_data
