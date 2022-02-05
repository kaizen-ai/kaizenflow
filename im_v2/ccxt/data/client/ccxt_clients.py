"""
Import as:

import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl
"""

import abc
import logging
import os
from typing import Any, List, Optional

import pandas as pd

import core.pandas_helpers as cpanh
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.ccxt.universe.universe as imvccunun
import im_v2.common.data.client as icdc

_LOG = logging.getLogger(__name__)

# Latest historical data snapshot.
_LATEST_DATA_SNAPSHOT = "20210924"

# TODO(gp): @Grisha These classes should return a `full_symbol` and not two
# columns `exchange_id` and `currency_pair`.

# #############################################################################
# CcxtCddClient
# #############################################################################


class CcxtCddClient(icdc.ImClient, abc.ABC):
    """
    Contain common code for all the `CCXT` and `CDD` clients, e.g.,

    - getting `CCXT` and `CDD` universe
    - applying common transformation for all the data from `CCXT` and `CDD`
        - E.g., `_apply_olhlcv_transformations()`, `_apply_vendor_normalization()`
    """

    def __init__(self, vendor: str) -> None:
        """
        Constructor.

        :param vendor: price data provider, i.e. `CCXT` or `CDD`
        """
        _vendors = ["CCXT", "CDD"]
        hdbg.dassert_in(vendor, _vendors)
        self._vendor = vendor
        super().__init__()

    def get_universe(self) -> List[icdc.FullSymbol]:
        """
        See description in the parent class.
        """
        universe = imvccunun.get_vendor_universe(vendor=self._vendor)
        return universe  # type: ignore[no-any-return]

    def _apply_vendor_normalization(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Input data is indexed with numbers and looks like:
        ```
             timestamp      open     high     low      close    volume    currency_pair exchange_id
        0    1631145600000  3499.01  3499.49  3496.17  3496.36  346.4812  ETH_USDT      binance
        1    1631145660000  3496.36  3501.59  3495.69  3501.59  401.9576  ETH_USDT      binance
        2    1631145720000  3501.59  3513.10  3499.89  3513.09  579.5656  ETH_USDT      binance
        ```
        Output data is indexed by timestamp and looks like:
        ```
                                   open        currency_pair exchange_id
        2021-09-08 20:00:00-04:00  3499.01 ... ETH_USDT      binance
        2021-09-08 20:01:00-04:00  3496.36     ETH_USDT      binance
        2021-09-08 20:02:00-04:00  3501.59     ETH_USDT      binance
        ```
        """
        # Apply vendor-specific transformations.
        data = self._apply_ccxt_cdd_normalization(data)
        # Apply transformations specific of the type of data.
        data = self._apply_ohlcv_transformations(data)
        # Sort transformed data by exchange id and currency pair columns.
        data = data.sort_values(by=["exchange_id", "currency_pair"])
        return data

    def _apply_ccxt_cdd_normalization(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformations common to `CCXT` and `CDD` data.
        """
        if self._vendor == "CDD":
            # Rename columns for consistency with other crypto vendors.
            # Column name for `volume` depends on the `currency_pair`, e.g., there are 2 columns
            # `Volume BTC` and `Volume USDT` for `currency pair `BTC_USDT. And there is no easy
            # way to select the right `Volume` column without passing `currency_pair` that will
            # complicate the interface. To get rid of this dependency the column's index is used.
            data.columns.values[7] = "volume"
            data = data.rename({"unix": "timestamp"}, axis=1)
        # Verify that the timestamp data is provided in ms.
        hdbg.dassert_container_type(
            data["timestamp"], container_type=None, elem_type=int
        )
        # Transform Unix epoch into UTC timestamp.
        data["timestamp"] = pd.to_datetime(data["timestamp"], unit="ms", utc=True)
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
            "currency_pair",
            "exchange_id",
        ]
        # Verify that dataframe contains OHLCV columns.
        hdbg.dassert_is_subset(ohlcv_columns, data.columns)
        # Rearrange the columns.
        data = data[ohlcv_columns]
        return data


# #############################################################################
# CcxtCddDbClient
# #############################################################################


# TODO(Grisha): it should descend from `ImClientReadingMultipleSymbols`.
class CcxtCddDbClient(CcxtCddClient, icdc.ImClientReadingOneSymbol):
    """
    `CCXT` client for data stored in an SQL database.
    """

    def __init__(
        self,
        vendor: str,
        connection: hsql.DbConnection,
    ) -> None:
        """
        Load `CCXT` and `CDD` price data from the database.

        This code path is typically used for the real-time data.

        :param vendor: price data provider, i.e. `CCXT` or `CDD`
        :param connection: connection for a SQL database
        """
        super().__init__(vendor)
        self._connection = connection

    def _read_data_for_one_symbol(
        self,
        full_symbol: icdc.FullSymbol,
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        **read_sql_kwargs: Any,
    ) -> pd.DataFrame:
        """
        Same as parent class.
        """
        table_name = self._vendor.lower() + "_ohlcv"
        # Verify that table with specified name exists.
        hdbg.dassert_in(table_name, hsql.get_table_names(self._connection))
        # Initialize SQL query.
        sql_query = "SELECT * FROM %s" % table_name
        # Split full symbol into exchange and currency pair.
        exchange_id, currency_pair = icdc.parse_full_symbol(full_symbol)
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
        # Normalize data according to the vendor.
        data = self._apply_vendor_normalization(data)
        return data


# #############################################################################
# CcxtFileSystemClient
# #############################################################################


class CcxtCddCsvParquetByAssetClient(
    CcxtCddClient, icdc.ImClientReadingOneSymbol
):
    """
    Read data from a CSV or Parquet file storing data for a single `CCXT` or
    `CDD` asset.

    It can read data from local or S3 filesystem as backend.

    Using our naming convention this class implements the two classes:
    - CcxtCddCsvClient
    - CcxtCddPqByAssetClient
    """

    def __init__(
        self,
        vendor: str,
        root_dir: str,
        # TODO(gp): -> file_extension
        extension: str,
        *,
        aws_profile: Optional[str] = None,
        data_snapshot: Optional[str] = None,
    ) -> None:
        """
        Load `CCXT` data from local or S3 filesystem.

        :param vendor: price data provider, i.e. `CCXT` or `CDD`
        :param root_dir: either a local root path (e.g., "/app/im") or
            an S3 root path (e.g., "s3://alphamatic-data/data") to `CCXT` data
        :param extension: file extension, e.g., `.csv`, `.csv.gz` or `.parquet`
        :param aws_profile: AWS profile name (e.g., "am")
        :param data_snapshot: snapshot of datetime when data was loaded,
            e.g. "20210924"
        """
        super().__init__(vendor)
        self._root_dir = root_dir
        # Verify that extension does not start with "." and set parameter.
        hdbg.dassert(
            not extension.startswith("."),
            "The extension %s should not start with '.'",
            extension,
        )
        self._extension = extension
        self._data_snapshot = data_snapshot or _LATEST_DATA_SNAPSHOT
        # Set s3fs parameter value if aws profile parameter is specified.
        if aws_profile:
            self._s3fs = hs3.get_s3fs(aws_profile)

    def _read_data_for_one_symbol(
        self,
        full_symbol: icdc.FullSymbol,
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        See the `_read_data_for_one_symbol()` in the parent class.
        """
        # Split full symbol into exchange and currency pair.
        exchange_id, currency_pair = icdc.parse_full_symbol(full_symbol)
        # Get absolute file path for a file with crypto price data.
        file_path = self._get_file_path(
            self._data_snapshot, exchange_id, currency_pair
        )
        # Read raw crypto price data.
        _LOG.info(
            "Reading data for vendor=`%s`, exchange id='%s', currencies='%s' from file='%s'...",
            self._vendor,
            exchange_id,
            currency_pair,
            file_path,
        )
        if hs3.is_s3_path(file_path):
            # Add s3fs argument to kwargs.
            kwargs["s3fs"] = self._s3fs
        if self._vendor == "CDD":
            # For `CDD` column names are in the 1st row.
            kwargs["skiprows"] = 1
        if self._extension == "pq":
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
                kwargs["filters"] = filters
            # Load data.
            data = cpanh.read_parquet(file_path, **kwargs)
        elif self._extension in ["csv", "csv.gz"]:
            data = cpanh.read_csv(file_path, **kwargs)
            # Filter by dates if specified.
            if start_ts:
                start_ts = hdateti.convert_timestamp_to_unix_epoch(start_ts)
                data = data[data["timestamp"] >= start_ts]
            if end_ts:
                end_ts = hdateti.convert_timestamp_to_unix_epoch(end_ts)
                data = data[data["timestamp"] < end_ts]
        else:
            raise ValueError(
                f"Unsupported extension {self._extension}. "
                f"Supported extensions are: `pq`, `csv`, `csv.gz`"
            )
        # Verify that required columns are not already in the dataframe.
        for col in ["exchange_id", "currency_pair"]:
            hdbg.dassert_not_in(col, data.columns)
        # Add required columns.
        data["exchange_id"] = exchange_id
        data["currency_pair"] = currency_pair
        # Normalize data according to the vendor.
        data = self._apply_vendor_normalization(data)
        return data

    def _get_file_path(
        self,
        data_snapshot: str,
        exchange_id: str,
        currency_pair: str,
    ) -> str:
        """
        Get the absolute path to a file with `CCXT` or `CDD` price data.

        The file path is constructed in the following way:
        `<root_dir>/<vendor>/<snapshot>/<exchange_id>/<currency_pair>.<self._extension>`

        :param data_snapshot: snapshot of datetime when data was loaded,
            e.g. "20210924"
        :param exchange_id: exchange id, e.g. "binance"
        :param currency_pair: currency pair `<currency1>_<currency2>`,
            e.g. "BTC_USDT"
        :return: absolute path to a file with `CCXT` or `CDD` price data
        """
        # Get absolute file path.
        file_name = ".".join([currency_pair, self._extension])
        file_path = os.path.join(
            self._root_dir,
            self._vendor.lower(),
            data_snapshot,
            exchange_id,
            file_name,
        )
        # TODO(Dan): Remove asserts below after CMTask108 is resolved.
        # Verify that the file exists.
        if hs3.is_s3_path(file_path):
            hs3.dassert_s3_exists(file_path, self._s3fs)
        else:
            hdbg.dassert_file_exists(file_path)
        return file_path
