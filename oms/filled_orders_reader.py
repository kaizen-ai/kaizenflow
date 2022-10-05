"""
Import as:

import oms.filled_orders_reader as ofiorrea
"""
import logging
import os
import re
from typing import Any, Dict, List, Union

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hio as hio
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
    ) -> List[str]:
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
        # Get files for the given time range.
        files = os.listdir(root_dir)
        # Get files for the target account, e.g. only for `***REMOVED***`.
        files = [f for f in files if str(self._secret_identifier) in f]
        # Search for filename date patterns, e.g.
        #  '20221001-000000_20221004-000000'.
        pattern = re.compile(r"(\d+-\d+)_(\d+-\d+)")
        # Construct a DataFrame of file names and corresponding timestamps.
        #
        # Select all found time ranges from files.
        start_ts_strings = []
        end_ts_strings = []
        for file in files:
            file_date_range = re.findall(pattern, file)[0]
            start_ts_as_str = file_date_range[0]
            end_ts_as_str = file_date_range[0]
            start_ts_strings.append(start_ts_as_str)
            end_ts_strings.append(end_ts_as_str)
        file_paths_df = pd.DataFrame(
            data={
                "file_path": files,
                "start_ts_string": start_ts_strings,
                "end_ts_string": end_ts_strings,
            }
        )
        file_paths_df[["start_ts_timestamp", "end_ts_timestamp"]] = file_paths_df[
            ["start_ts_string", "end_ts_string"]
        ]
        #
        if start_ts < file_paths_df["start_ts_timestamp"].min():
            file_paths_df = file_paths_df.loc[
                file_paths_df["start_ts_timestamp"].min()
            ]
            _LOG.warning(
                "Provided start_ts is earlier than the earliest data timestamp: %s",
                file_paths_df["start_ts_timestamp"].min(),
            )
        file_paths_df = file_paths_df.loc[
            file_paths_df["start_ts_timestamp"] >= start_ts
        ]
        #
        if end_ts > file_paths_df["end_ts_timestamp"].max():
            file_paths_df = file_paths_df.loc[
                file_paths_df["end_ts_timestamp"].min()
            ]
            _LOG.warning(
                "Provided end_ts is later than the latest data timestamp: %s",
                file_paths_df["end_ts_timestamp"].max(),
            )
        file_paths_df = file_paths_df.loc[
            file_paths_df["end_ts_timestamp"] <= end_ts
        ]
        file_paths = file_paths_df["file_paths"]
        file_paths = [(os.path.join(root_dir, f)) for f in file_paths]
        return file_paths

    # TODO(Danya): Update to use with JSON.
    def read_filled_orders(
        self, start_ts: pd.Timestamp, end_ts: pd.Timestamp, file_format: str
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
        file_names = self.get_file_names_for_time_period(
            start_ts, end_ts, file_format
        )
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
