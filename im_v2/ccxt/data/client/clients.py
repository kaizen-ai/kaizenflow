"""
Import as:

import im_v2.ccxt.data.client.clients as imvcdclcl
"""

# TODO(gp): -> ccxt_clients.py to try to make the names unique, even if there is
#  stuttering.

import abc
import logging
import os
from typing import Any, List, Optional

import pandas as pd

import core.pandas_helpers as cpanh
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.ccxt.universe.universe as imvccunun
import im_v2.common.data.client as imvcdcli

_LOG = logging.getLogger(__name__)

# Latest historical data snapshot.
_LATEST_DATA_SNAPSHOT = "20210924"


# TODO(gp): @grisha, Move inside AbstractCcxtClient since it's private to
#  that class.
_DATA_TYPES = ["ohlcv"]


# #############################################################################
# AbstractCcxtClient
# #############################################################################

# CcxtClient
#   CcxtPqByDate...
#   CcxtPqByAsset..
#   CcxtDb...
#   CcxtCsv...
#   CcxtPqByAsset..


# TODO(gp): Consider splitting this file into chunks

class CcxtClient(imvcdcli.AbstractImClient, abc.ABC):
    """
    Abstract interface for CCXT client.

    Contain common code for all the CCXT clients, e.g.,
    - getting CCXT universe
    - applying common transformation for all the data from CCXT
        - E.g., `_apply_olhlcv_transformations()`, `_apply_vendor_normalization()`
    """

    def __init__(self, data_type: str) -> None:
        """
        :param data_type: OHLCV, trade, or bid/ask data
        """
        data_type = data_type.lower()
        hdbg.dassert_in(data_type, _DATA_TYPES)
        self._data_type = data_type

    @staticmethod
    def get_universe() -> List[imvcdcli.FullSymbol]:
        """
        Return CCXT universe as full symbols.
        """
        universe = imvccunun.get_vendor_universe(vendor="CCXT")
        return universe  # type: ignore[no-any-return]

    @staticmethod
    def _apply_ccxt_transformations(data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformations common to all CCXT data.
        """
        # Verify that the timestamp data is provided in ms.
        hdbg.dassert_container_type(
            data["timestamp"], container_type=None, elem_type=int
        )
        # Rename column with the original Unix ms epoch.
        # TODO(gp): Remove epoch.
        data = data.rename({"timestamp": "epoch"}, axis=1)
        # Transform Unix epoch into UTC timestamp.
        data["timestamp"] = pd.to_datetime(data["epoch"], unit="ms", utc=True)
        # Set timestamp as index.
        data = data.set_index("timestamp")
        return data

    @staticmethod
    def _apply_ohlcv_transformations(data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformations for OHLCV data.
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
        # TODO(gp): dassert_is_seteq?
        hdbg.dassert_is_subset(ohlcv_columns, data.columns)
        # Rearrange the columns.
        # TODO(gp): Why copying?
        data = data[ohlcv_columns].copy()
        return data

    def _apply_vendor_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        See description in the parent class.

        Input data is indexed with numbers and looks like:
        ```
             timestamp      open     high     low      close    volume    currency_pair exchange_id
        0    1631145600000  3499.01  3499.49  3496.17  3496.36  346.4812  ETH_USDT      binance
        1    1631145660000  3496.36  3501.59  3495.69  3501.59  401.9576  ETH_USDT      binance
        2    1631145720000  3501.59  3513.10  3499.89  3513.09  579.5656  ETH_USDT      binance
        ```

        Output data is indexed by timestamp and looks like:
        ```
                                   open        epoch          currency_pair exchange_id
        2021-09-08 20:00:00-04:00  3499.01 ... 1631145600000  ETH_USDT      binance
        2021-09-08 20:01:00-04:00  3496.36     1631145660000  ETH_USDT      binance
        2021-09-08 20:02:00-04:00  3501.59     1631145720000  ETH_USDT      binance
        ```
        """
        # Apply common transformations.
        data = self._apply_ccxt_transformations(df)
        # Apply transformations specific of the type of data.
        if self._data_type == "ohlcv":
            data = self._apply_ohlcv_transformations(data)
        else:
            raise ValueError(
                "Incorrect data type: '%s'. Acceptable types: '%s'"
                % (self._data_type, _DATA_TYPES)
            )
        # Sort transformed data by exchange id and currency pair columns.
        data = data.sort_values(by=["exchange_id", "currency_pair"])
        return data


# #############################################################################
# CcxtDbClient
# #############################################################################

# TODO(Grisha): it should descend from `ImClientReadingMultipleSymbols`.
class CcxtDbClient(AbstractCcxtClient, imvcdcli.ImClientReadingOneSymbol):
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
        super().__init__(data_type)
        self._connection = connection

    def _read_data_for_one_symbol(
        self,
        full_symbol: imvcdcli.FullSymbol,
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        **read_sql_kwargs: Any,
    ) -> pd.DataFrame:
        """
        Same as parent class.
        """
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
# AbstractCcxtFileSystemClient
# #############################################################################


# TODO(gp): @grisha -> CcxtFileSystemClient to simplify the naming scheme.
class CcxtCsvPqByAssetClient(abc.ABC, ImClientReadingOneSymbol):
    """
    CCXT client that reads CSV or Parquet file storing data for a single asset.

    It can read data from local or S3 filesystem as backend.

    Using our naming convention this class implements the two classes:
    - CcxtCsvClient
    - CcxtPqByAssetClient
    """

    def __init__(
        self,
        # TODO(gp): It might be premature to try to handle OHLCV, order book data
        #  and so on. Consider focusing only on OHLCV.
        data_type: str,
        root_dir: str,
        extension: str,
        data_snapshot: Optional[str] = None,
        *,
        aws_profile: Optional[str] = None,
    ) -> None:
        """
        Constructor.

        :param data_type: OHLCV data or order book.
        :param root_dir: either a local root path (e.g., "/app/im") or
            an S3 root path (e.g., "s3://alphamatic-data/data") to CCXT data
        :param extension: file extension, e.g., `.csv` or `.parquet`
        :param data_snapshot: snapshot of datetime when data was loaded,
            e.g. "20210924"
        :param aws_profile: AWS profile name (e.g., "am")
        """
        super().__init__(data_type)
        self._root_dir = root_dir
        # Verify that extension does not start with "." and set parameter.
        hdbg.dassert(
            not extension.startswith("."),
            "The extension %s should not start with '.'" % extension,
        )
        self._extension = extension
        #
        self._data_snapshot = self._data_snapshot or _LATEST_DATA_SNAPSHOT
        # Set s3fs parameter value if aws profile parameter is specified.
        if aws_profile:
            self._s3fs = hs3.get_s3fs(aws_profile)

    def _read_data_for_one_symbol(
        self,
        full_symbol: imvcdcli.FullSymbol,
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
    ) -> pd.DataFrame:
        """
        Same as parent class.
        """
        # Split full symbol into exchange and currency pair.
        exchange_id, currency_pair = imvcdcli.parse_full_symbol(full_symbol)
        # Get absolute file path for a CCXT file including the extension.
        file_path = self._get_file_path(data_snapshot, exchange_id, currency_pair)
        # Initialize kwargs dict for further CCXT data reading.
        read_kwargs = {}
        if hs3.is_s3_path(file_path):
            # Add s3fs argument to kwargs.
            read_kwargs["s3fs"] = self._s3fs
        # Read raw CCXT data.
        _LOG.info(
            "Reading CCXT data for exchange id='%s', currencies='%s' from file='%s'...",
            exchange_id,
            currency_pair,
            file_path,
        )
        if self.extension == "pq":
            # Initialize list of filters.
            filters = []
            if start_ts:
                # Add filtering by start timestamp if specified.
                start_ts = hdateti.convert_timestamp_to_unix_epoch(start_ts)
                filters.append(("timestamp", ">=", start_ts))
            if end_ts:
                # Add filtering by end timestamp if specified.
                end_ts = hdateti.convert_timestamp_to_unix_epoch(end_ts)
                filters.append(("timestamp", "<", end_ts))
            if filters:
                # Add filters to kwargs if any were set.
                read_kwargs["filters"] = filters
            # Load data.
            data = cpanh.read_parquet(file_path, **read_kwargs)
        elif self.extension == "csv":
            data = cpanh.read_csv(file_path, **read_kwargs)
            # Filter by dates if specified.
            if start_ts:
                start_ts = hdateti.convert_timestamp_to_unix_epoch(start_ts)
                data = data[data["timestamp"] >= start_ts]
            if end_ts:
                end_ts = hdateti.convert_timestamp_to_unix_epoch(end_ts)
                data = data[data["timestamp"] < end_ts]
        return data

    # TODO(Grisha): factor out common code from `CddLoader._get_file_path` and
    #  `CcxtLoader._get_file_path`.
    def _get_file_path(
        self,
        data_snapshot: str,
        exchange_id: str,
        currency_pair: str,
    ) -> str:
        """
        Get the absolute path to a file with CCXT data.

        The file path is constructed in the following way:
        `<root_dir>/ccxt/<snapshot>/<exchange_id>/<currency_pair>.<self._extension>`

        :param data_snapshot: snapshot of datetime when data was loaded,
            e.g. "20210924"
        :param exchange_id: CCXT exchange id, e.g. "binance"
        :param currency_pair: currency pair `<currency1>_<currency2>`,
            e.g. "BTC_USDT"
        :return: absolute path to a file with CCXT data
        """
        # Get absolute file path.
        file_name = ".".join([currency_pair, self._extension])
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


# #############################################################################
# CcxtCsvFileSystemClient
# #############################################################################


# # Merge with the class above.
# class CcxtCsvFileSystemClient(AbstractCcxtFileSystemClient, imvcdcli.ImClientReadingOneSymbol):
#     """
#     CCXT client for data stored as CSV from local or S3 filesystem.
#
#     Each CSV file stores data for a single symbol so we use `Ccx
#     """
#
#     def __init__(
#         self,
#         data_type: str,
#         root_dir: str,
#         *,
#         aws_profile: Optional[str] = None,
#         use_gzip: bool = True,
#     ) -> None:
#         _LOG.debug(hprint.to_str("data_type root_dir aws_profile use_gzip"))
#         extension = "csv"
#         if use_gzip:
#             extension = extension + ".gz"
#         super().__init__(data_type, root_dir, extension, aws_profile=aws_profile)
#
#     @staticmethod
#     def _read_data_from_filesystem(
#         file_path: str,
#         start_ts: Optional[pd.Timestamp],
#         end_ts: Optional[pd.Timestamp],
#         **read_kwargs: Any,
#     ) -> pd.DataFrame:
#         """
#         Same params as the parent class.
#         """
#         _LOG.debug(hprint.to_str("file_path start_ts end_ts"))
#         # Load data.
#         data = cpanh.read_csv(file_path, **read_kwargs)
#         # Filter by dates if specified.
#         if start_ts:
#             start_ts = hdateti.convert_timestamp_to_unix_epoch(start_ts)
#             data = data[data["timestamp"] >= start_ts]
#         if end_ts:
#             end_ts = hdateti.convert_timestamp_to_unix_epoch(end_ts)
#             data = data[data["timestamp"] < end_ts]
#         return data


# #############################################################################
# CcxtParquetFileSystemClient
# #############################################################################


# # TODO(gp): @grisha This should descend from `ImClientReadingMultipleSymbols`
# #  since it reads PQ files.
# class CcxtParquetFileSystemClient(AbstractCcxtFileSystemClient, imvcdcli.ImClientReadingMultipleSymbols):
#     """
#     CCXT client for data stored as Parquet from local or S3 filesystem.
#     """
#
#     def __init__(
#         self,
#         data_type: str,
#         root_dir: str,
#         *,
#         aws_profile: Optional[str] = None,
#     ) -> None:
#         extension = "pq"
#         super().__init__(data_type, root_dir, extension, aws_profile=aws_profile)
#
#     @staticmethod
#     def _read_data_from_filesystem(
#         file_path: str,
#         start_ts: Optional[pd.Timestamp],
#         end_ts: Optional[pd.Timestamp],
#         **read_kwargs: Any,
#     ) -> pd.DataFrame:
#         """
#         See the `_read_data_from_filesystem()` in the parent class.
#         """
#         # Initialize list of filters.
#         filters = []
#         if start_ts:
#             # Add filtering by start timestamp if specified.
#             start_ts = hdateti.convert_timestamp_to_unix_epoch(start_ts)
#             filters.append(("timestamp", ">=", start_ts))
#         if end_ts:
#             # Add filtering by end timestamp if specified.
#             end_ts = hdateti.convert_timestamp_to_unix_epoch(end_ts)
#             filters.append(("timestamp", "<", end_ts))
#         if filters:
#             # Add filters to kwargs if any were set.
#             read_kwargs["filters"] = filters
#         # Load data.
#         data = cpanh.read_parquet(file_path, **read_kwargs)
#         return data
