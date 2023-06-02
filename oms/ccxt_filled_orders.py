"""
Import as:

import oms.ccxt_filled_orders as occfiord
"""
import logging
import os
import re
from typing import Any, Dict, List

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hio as hio
import oms.hsecrets as homssec

_LOG = logging.getLogger(__name__)

# #############################################################################
# Filled Orders Reader
# #############################################################################


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

    def read_json_orders(
        self, start_ts: pd.Timestamp, end_ts: pd.Timestamp
    ) -> List[Dict[str, Any]]:
        """
        Read a JSON file for filled trades.

        Example of JSON output data: same as `FilledTradesReader.convert_fills_json_to_dataframe`.

        :param start_ts: beginning of time period, tz-aware
        :param end_ts: end of time period, tz-aware
        :return: JSON list of dicts for 'json', formatted DataFrame for 'csv'
        """
        # Assert that passed timestamps have timezones.
        hdateti.dassert_has_tz(start_ts)
        hdateti.dassert_has_tz(end_ts)
        file_format = "json"
        # Get files corresponding to the given time period.
        file_names = self._get_file_names_for_time_period(
            start_ts, end_ts, file_format
        )
        filled_trades_data = []
        # Load JSON as a list of dicts.
        for file_name in file_names:
            data = hio.from_json(file_name)
            filled_trades_data.extend(data)
        return filled_trades_data

    def read_csv_orders(
        self,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> pd.DataFrame:
        # pylint: disable=line-too-long
        """
        Read a CSV file for filled trades.

        Example of CSV output data:
                                                    symbol         id       order  side takerOrMaker  price  amount    cost      fees fees_currency  realized_pnl
        timestamp
        2022-09-29 16:46:39.509000+00:00  APE/USDT  282773274  5772340563  sell        taker  5.427     5.0  27.135  0.010854          USDT         0.000
        2022-09-29 16:51:58.567000+00:00  APE/USDT  282775654  5772441841   buy        taker  5.398     6.0  32.388  0.012955          USDT         0.145
        2022-09-29 16:57:00.267000+00:00  APE/USDT  282779084  5772536135   buy        taker  5.407     3.0  16.221  0.006488          USDT         0.000

        :param start_ts: beginning of time period, tz-aware
        :param end_ts: end of time period, tz-aware
        :return: JSON list of dicts for 'json', formatted DataFrame for 'csv'
        """
        # pylint: enable=line-too-long
        # Assert that passed timestamps have timezones.
        hdateti.dassert_has_tz(start_ts)
        hdateti.dassert_has_tz(end_ts)
        # Get files corresponding to the given time period.
        file_format = "csv"
        file_names = self._get_file_names_for_time_period(
            start_ts, end_ts, file_format
        )
        filled_trades_data = []
        # Load CSV files as a dataframe.
        for file_name in file_names:
            df = pd.read_csv(file_name, parse_dates=["timestamp"])
            filled_trades_data.append(df)
        filled_trades_data = pd.concat(filled_trades_data)
        # Filter data outside the given time period.
        filled_trades_data = filled_trades_data.loc[
            (filled_trades_data["timestamp"] >= start_ts)
            & (filled_trades_data["timestamp"] <= end_ts)
        ]
        # Set timestamp index
        filled_trades_data = filled_trades_data.set_index("timestamp")
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

    def _get_file_names_for_time_period(
        self, start_ts: pd.Timestamp, end_ts: pd.Timestamp, file_format: str
    ) -> List[str]:
        """
        Get files that correspond to the given time period.

        The time period is calculated as `[a,b]`.

        Example of a file name:
        'fills_20221001-000000_20221004-000000_***REMOVED***.csv.gz'

        :param start_ts: start of time period
        :param end_ts: end of time period
        :param file_format: 'json' or 'csv'
        :return: list of files falling into given time range
        """
        hdbg.dassert_in(file_format, ["csv", "json"])
        # Get files of the given file format.
        root_dir = os.path.join(self._root_dir, file_format)
        file_names = os.listdir(root_dir)
        # Select files for the target account, e.g. only for `***REMOVED***`.
        file_names = [f for f in file_names if str(self._secret_identifier) in f]
        # Search for date patterns in file names, e.g.
        #  '20221001-000000_20221004-000000'.
        pattern = re.compile(r"(\d+-\d+)_(\d+-\d+)")
        # Construct a DataFrame of file names and corresponding timestamps.
        #
        # Select all found time ranges from files.
        start_ts_strings = []
        end_ts_strings = []
        for file in file_names:
            # Extract start and end of the time range from file name.
            #  e.g. ('20221001-000000', '20221004-000000')
            file_date_range = re.findall(pattern, file)[0]
            start_ts_as_str = file_date_range[0]
            end_ts_as_str = file_date_range[1]
            start_ts_strings.append(start_ts_as_str)
            end_ts_strings.append(end_ts_as_str)
        # Construct a dataframe for ease of filtering and convert timestamps.
        file_paths_df = pd.DataFrame(
            data={
                "file_path": file_names,
                "start_ts": pd.to_datetime(start_ts_strings, utc=True),
                "end_ts": pd.to_datetime(end_ts_strings, utc=True),
            }
        )
        # Filter data by start_ts.
        if start_ts < file_paths_df["start_ts"].min():
            _LOG.warning(
                "Provided start_ts is earlier than the earliest data timestamp: %s",
                file_paths_df["start_ts"].min(),
            )
        file_paths_df = file_paths_df.loc[file_paths_df["start_ts"] >= start_ts]
        # Filter data by end_ts.
        if end_ts > file_paths_df["end_ts"].max():
            _LOG.warning(
                "Provided end_ts is later than the latest data timestamp: %s",
                file_paths_df["end_ts"].max(),
            )
        file_paths_df = file_paths_df.loc[file_paths_df["end_ts"] <= end_ts]
        # Extract file paths.
        file_paths = file_paths_df["file_path"].to_list()
        # Get absolute paths.
        file_paths = [(os.path.join(root_dir, f)) for f in file_paths]
        return file_paths


# #############################################################################
# Filled Orders and TCA utilities
# #############################################################################


# TODO(Danya): Add test.
def convert_fills_json_to_dataframe(
    fills_json: List[Dict[str, Any]]
) -> pd.DataFrame:
    """
    Convert JSON-format fills into a dataframe.

    - Unpack nested values;
    - Convert unix epoch to pd.Timestamp;
    - Remove duplicated information;

    Example of output data:
                             timestamp    symbol         id       order  side  \
    0 2022-09-29 16:46:39.509000+00:00  APE/USDT  282773274  5772340563  sell
    1 2022-09-29 16:51:58.567000+00:00  APE/USDT  282775654  5772441841   buy
    2 2022-09-29 16:57:00.267000+00:00  APE/USDT  282779084  5772536135   buy
    3 2022-09-29 17:02:00.329000+00:00  APE/USDT  282780259  5772618089  sell
    4 2022-09-29 17:07:03.097000+00:00  APE/USDT  282781536  5772689853   buy

      takerOrMaker  price  amount    cost      fees fees_currency realized_pnl
    0        taker  5.427     5.0  27.135  0.010854          USDT            0
    1        taker  5.398     6.0  32.388  0.012955          USDT   0.14500000
    2        taker  5.407     3.0  16.221  0.006488          USDT            0
    3        taker  5.395     9.0  48.555  0.019422          USDT  -0.03900000
    4        taker  5.381     8.0  43.048  0.017219          USDT   0.07000000

    Example of input JSON can be found at CcxtBroker.get_filled_trades().
    """
    fills = pd.DataFrame(fills_json)
    # Extract nested values.
    fills["fees"] = [d["cost"] for d in fills["fee"]]
    fills["fees_currency"] = [d["currency"] for d in fills["fee"]]
    fills["realized_pnl"] = [d["realizedPnl"] for d in fills["info"]]
    # Replace unix epoch with a timestamp.
    fills["timestamp"] = fills["timestamp"].apply(
        hdateti.convert_unix_epoch_to_timestamp
    )
    # Set columns.
    columns = [
        "timestamp",
        "symbol",
        "asset_id",
        "id",
        "order",
        "side",
        "takerOrMaker",
        "price",
        "amount",
        "cost",
        "fees",
        "fees_currency",
        "realized_pnl",
    ]
    fills = fills[columns]
    # Set timestamp index.
    fills = fills.set_index("timestamp")
    return fills
