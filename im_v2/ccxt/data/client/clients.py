"""
Import as:

import im_v2.ccxt.data.client.clients as imcdaclcl
"""

import abc
import logging
import os
from typing import Any, Dict, List, Optional, Union

import pandas as pd

import core.pandas_helpers as cpanh
import helpers.datetime_ as hdateti
import helpers.dbg as hdbg
import helpers.hpandas as hpandas
import helpers.s3 as hs3
import helpers.sql as hsql
import im_v2.common.data.client as imvcdcli
import im_v2.common.universe.universe as imvcounun

_LOG = logging.getLogger(__name__)

# Latest historical data snapshot.
_LATEST_DATA_SNAPSHOT = "20210924"
#
_DATA_TYPES = ["ohlcv"]


class AbstractCcxtClient(imvcdcli.AbstractImClient, abc.ABC):
    """
    Abstract Interface for CCXT client.
    """
    def __init__(self, data_type: str) -> None:
        """
        :param data_type: OHLCV or trade, bid/ask data
        """
        date_type_lower = data_type.lower()
        hdbg.dassert_in(date_type_lower, _DATA_TYPES)
        self._data_type = date_type_lower

    def _normalize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        See description in the parent class.

        Input data is indexed with numbers and contains the columns timestamp
        open, high, low, closed, volume, exchange_id, currency_pair e.g.,
        ```
             timestamp      open     high     low      close    volume    currency_pair exchange_id
        0    1631145600000  3499.01  3499.49  3496.17  3496.36  346.4812  ETH/USDT      binance
        1    1631145660000  3496.36  3501.59  3495.69  3501.59  401.9576  ETH/USDT      binance
        2    1631145720000  3501.59  3513.10  3499.89  3513.09  579.5656  ETH/USDT      binance
        ```

        Output data is indexed by timestamp and contains the columns open,
        high, low, close, volume, epoch, currency_pair, exchange_id, e.g.,
        ```
                                   open        epoch          currency_pair exchange_id
        2021-09-08 20:00:00-04:00  3499.01 ... 1631145600000  ETH/USDT      binance
        2021-09-08 20:01:00-04:00  3496.36     1631145660000  ETH/USDT      binance
        2021-09-08 20:02:00-04:00  3501.59     1631145720000  ETH/USDT      binance
        ```
        """
        # Apply common transformations.
        transformed_data = self._apply_common_transformation(df)
        # Apply transformations for OHLCV data.
        if self._data_type == "ohlcv":
            transformed_data = self._apply_ohlcv_transformation(transformed_data)
        else:
            hdbg.dfatal(
                "Incorrect data type: '%s'. Acceptable types: '%s'"
                % (self._data_type, _DATA_TYPES)
            )
        # Sort transformed data by exchange id and currency pair columns.
        transformed_data = transformed_data.sort_values(
            by=["exchange_id", "currency_pair"]
        )
        return transformed_data

    def _apply_common_transformation(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformations common to all CCXT data.

        This includes:
        - Datetime format assertion
        - Converting epoch ms timestamp to `pd.Timestamp`
        - Converting `timestamp` to index

        :param data: raw CCXT data
        :return: transformed CCXT data
        """
        # Verify that the timestamp data is provided in ms.
        hdbg.dassert_container_type(
            data["timestamp"], container_type=None, elem_type=int
        )
        # Rename col with original Unix ms epoch.
        data = data.rename({"timestamp": "epoch"}, axis=1)
        # Transform Unix epoch into ET timestamp.
        data["timestamp"] = self._convert_epochs_to_timestamp(data["epoch"])
        # Set timestamp as index.
        data = data.set_index("timestamp")
        return data

    @staticmethod
    def _convert_epochs_to_timestamp(epoch_col: pd.Series) -> pd.Series:
        """
        Convert Unix epoch to timestamp in ET.

        All Unix time epochs in CCXT are provided in ms and in UTC tz.

        :param epoch_col: Series with Unix time epochs
        :return: Series with epochs converted to timestamps in ET
        """
        # Convert to timestamp in UTC tz.
        timestamp_col = pd.to_datetime(epoch_col, unit="ms", utc=True)
        # Convert to ET tz.
        timestamp_col = timestamp_col.dt.tz_convert(hdateti.get_ET_tz())
        return timestamp_col

    @staticmethod
    def _apply_ohlcv_transformation(data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformations for OHLCV data.

        This includes:
        - Assertion of present columns
        - Assertion of data types
        - Renaming and rearranging of OHLCV columns, namely:
            ["open",
             "high",
             "low",
             "close"
             "volume",
             "epoch",
             "currency_pair",
             "exchange_id"]

        :param data: OHLCV data
        :return: transformed OHLCV data
        """
        ohlcv_columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "epoch",
            "currency_pair",
            "exchange_id",
        ]
        # Verify that dataframe contains OHLCV columns.
        hdbg.dassert_is_subset(ohlcv_columns, data.columns)
        # Rearrange the columns.
        data = data[ohlcv_columns].copy()
        return data


# #############################################################################


class CcxtDbClient(AbstractCcxtClient):
    """
    CCXT client for data from the database.
    """
    def __init__(
        self,
        data_type: str,
        connection: hsql.DbConnection,
    ) -> None:
        """
        Load CCXT data from the database.

        This code path is used for the real-time data.

        :param connection: connection for a SQL database
        """
        super().__init__(data_type=data_type)
        self._connection = connection

    def _read_data(
        self,
        full_symbol: imvcdcli.FullSymbol,
        start_ts: Optional[pd.Timestamp] = None,
        end_ts: Optional[pd.Timestamp] = None,
        **read_sql_kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        # Construct name of the DB table with data from data type.
        table_name = "ccxt_" + self._data_type
        # Verify that table with specified name exists.
        hdbg.dassert_in(table_name, hsql.get_table_names(self._connection))
        # Initialize SQL query.
        sql_query = "SELECT * FROM %s" % table_name
        # Split full symbol into exchange and currency pair.
        exchange_id, currency_pair = imvcdcli.parse_full_symbol(full_symbol)
        # Initialize a list for SQL conditions.
        sql_conditions = []
        # Fill SQL conditions list for each provided data parameter.
        sql_conditions.append(f"exchange_id = '{exchange_id}'")
        sql_conditions.append(f"currency_pair = '{currency_pair}'")
        if start_ts:
            start_ts = hdateti.convert_timestamp_to_unix_epoch(start_ts)
            sql_conditions.append(f"timestamp >= {start_ts}")
        if end_ts:
            end_ts = hdateti.convert_timestamp_to_unix_epoch(end_ts)
            sql_conditions.append(f"timestamp < {end_ts}")
        # Append all the provided SQL conditions to the main SQL query.
        sql_conditions = " AND ".join(sql_conditions)
        sql_query = " WHERE ".join([sql_query, sql_conditions])
        # Execute SQL query.
        data = pd.read_sql(sql_query, self._connection, **read_sql_kwargs)
        return data


# #############################################################################

class CcxtFileSystemClient(AbstractCcxtClient):
    """
    CCXT client for data from local or S3 filesystem.
    """
    def __init__(
        self,
        data_type: str,
        root_dir: str,
        aws_profile: Optional[str] = None,
    ) -> None:
        """
        Load CCXT data from local or S3 filesystem.

        :param: root_dir: either a local root path (e.g., "/app/im") or
            an S3 root path ("s3://alphamatic-data/data") to the CCXT data
        :param: aws_profile: AWS profile name (e.g., "am")
        """
        super().__init__(data_type=data_type)
        self._root_dir = root_dir
        # Set s3fs parameter value if aws profile parameter is specified.
        if aws_profile:
            self._s3fs = hs3.get_s3fs(aws_profile)

    def _read_data(
        self,
        full_symbol: imvcdcli.FullSymbol,
        start_ts: Optional[pd.Timestamp] = None,
        end_ts: Optional[pd.Timestamp] = None,
        data_snapshot: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Load data from a filesystem and process it for use downstream.

        :param data_snapshot: snapshot of datetime when data was loaded,
            e.g. "20210924"
        :return: processed CCXT data
        """
        data_snapshot = data_snapshot or _LATEST_DATA_SNAPSHOT
        # Split full symbol into exchange and currency pair.
        exchange_id, currency_pair = imvcdcli.parse_full_symbol(full_symbol)
        # Get absolute file path for a CCXT file.
        file_path = self._get_file_path(data_snapshot, exchange_id, currency_pair)
        # Initialize kwargs dict for further CCXT data reading.
        read_csv_kwargs = {}
        if hs3.is_s3_path(file_path):
            # Add s3fs argument to kwargs.
            read_csv_kwargs["s3fs"] = self._s3fs
        # Read raw CCXT data.
        _LOG.info(
            "Reading CCXT data for exchange id='%s', currencies='%s' from file='%s'...",
            exchange_id,
            currency_pair,
            file_path,
        )
        data = cpanh.read_csv(file_path, **read_csv_kwargs)
        #
        if start_ts:
            start_ts = hdateti.convert_timestamp_to_unix_epoch(start_ts)
            data = data[data["timestamp"] >= start_ts]
        if end_ts:
            end_ts = hdateti.convert_timestamp_to_unix_epoch(end_ts)
            data = data[data["timestamp"] < end_ts]
        # Apply transformation to raw data.
        _LOG.info(
            "Processing CCXT data for exchange id='%s', currencies='%s'...",
            exchange_id,
            currency_pair,
        )
        processed_data = self._preprocess_filesystem_data(data, exchange_id, currency_pair)
        return processed_data

    # TODO(Grisha): factor out common code from `CddLoader._get_file_path` and
    # `CcxtLoader._get_file_path`.
    def _get_file_path(
        self,
        data_snapshot: str,
        exchange_id: str,
        currency_pair: str,
    ) -> str:
        """
        Get the absolute path to a file with CCXT data.

        The file path is constructed in the following way:
        `<root_dir>/ccxt/<snapshot>/<exchange_id>/<currency_pair>.csv.gz`.

        :param data_snapshot: snapshot of datetime when data was loaded,
            e.g. "20210924"
        :param exchange_id: CCXT exchange id, e.g. "binance"
        :param currency_pair: currency pair `<currency1>/<currency2>`,
            e.g. "BTC_USDT"
        :return: absolute path to a file with CCXT data
        """
        # Get absolute file path.
        file_name = currency_pair + ".csv.gz"
        file_path = os.path.join(
            self._root_dir, "ccxt", data_snapshot, exchange_id, file_name
        )
        # TODO(Dan): Remove asserts below after CMTask108 is resolved.
        # Verify that the file exists.
        if hs3.is_s3_path(file_path):
            hs3.dassert_s3_exists(file_path, self._s3fs)
        else:
            hdbg.dassert_file_exists(file_path)
        return file_path

    @staticmethod
    def _preprocess_filesystem_data(
        data: pd.DataFrame,
        exchange_id: str,
        currency_pair: str,
    ) -> pd.DataFrame:
        """
        Preprocess filesystem data before transformation stage.

        This includes:
        - Adding exchange_id and currency_pair columns

        :param data: data from a filesystem
        :param exchange_id: CCXT exchange id, e.g. "binance"
        :param currency_pair: currency pair, e.g. "BTC_USDT"
        :return: preprocessed filesystem data
        """
        # Verify that required columns are not already in the dataframe.
        for col in ["exchange_id", "currency_pair"]:
            hdbg.dassert_not_in(col, data.columns)
        # Add required columns.
        data["exchange_id"] = exchange_id
        data["currency_pair"] = currency_pair
        return data
