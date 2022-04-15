"""
Import as:

import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl
"""

import abc
import collections
import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.common.data.client as icdc
import im_v2.common.universe as icunv

_LOG = logging.getLogger(__name__)

# Latest historical data snapshot.
_LATEST_DATA_SNAPSHOT = "20210924"
# TODO(gp): @all bump up to the new snapshot.
# _LATEST_DATA_SNAPSHOT = "20220210"


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

    def __init__(self, vendor: str, resample_1min: bool) -> None:
        """
        Constructor.
        """
        super().__init__(vendor, resample_1min)
        _vendors = ["CCXT", "CDD"]
        hdbg.dassert_in(self._vendor, _vendors)

    def get_universe(self) -> List[icdc.FullSymbol]:
        """
        See description in the parent class.
        """
        universe = icunv.get_vendor_universe(
            vendor=self._vendor, as_full_symbol=True
        )
        return universe  # type: ignore[no-any-return]

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
        ]
        # Verify that dataframe contains OHLCV columns.
        hdbg.dassert_is_subset(ohlcv_columns, data.columns)
        # Rearrange the columns.
        data = data[ohlcv_columns]
        return data

    def _apply_vendor_normalization(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Input data is indexed with numbers and looks like:
        ```
             timestamp      open        close    volume
        0    1631145600000  3499.01 ... 3496.36  346.4812
        1    1631145660000  3496.36     3501.59  401.9576
        2    1631145720000  3501.59     3513.09  579.5656
        ```
        Output data is indexed by timestamp and looks like:
        ```
                                   open        close    volume
        2021-09-08 20:00:00-04:00  3499.01 ... 3496.36  346.4812
        2021-09-08 20:01:00-04:00  3496.36     3501.59  401.9576
        2021-09-08 20:02:00-04:00  3501.59     3513.09  579.5656
        ```
        """
        # Apply vendor-specific transformations.
        data = self._apply_ccxt_cdd_normalization(data)
        # Apply transformations specific of the type of data.
        data = self._apply_ohlcv_transformations(data)
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
        resample_1min: bool,
        connection: hsql.DbConnection,
    ) -> None:
        """
        Load `CCXT` and `CDD` price data from the database.

        This code path is typically used for the real-time data.

        :param connection: connection for a SQL database
        """
        super().__init__(vendor, resample_1min)
        self._connection = connection

    def get_metadata(self) -> pd.DataFrame:
        """
        See description in the parent class.
        """
        raise NotImplementedError

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
            sql_conditions.append(f"timestamp <= {end_ts}")
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
        resample_1min: bool,
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
        super().__init__(vendor, resample_1min)
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

    def get_metadata(self) -> pd.DataFrame:
        """
        See description in the parent class.
        """
        raise NotImplementedError

    def _read_data_for_one_symbol(
        self,
        full_symbol: icdc.FullSymbol,
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        See description in the parent class.
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
        # TODO(Nikola): parquet?
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
                filters.append(("timestamp", "<=", end_ts))
            if filters:
                # Add filters to kwargs if any were set.
                kwargs["filters"] = filters
            # Load data.
            stream, kwargs = hs3.get_local_or_s3_stream(file_path, **kwargs)
            data = hpandas.read_parquet_to_df(stream, **kwargs)
        elif self._extension in ["csv", "csv.gz"]:
            stream, kwargs = hs3.get_local_or_s3_stream(file_path, **kwargs)
            data = hpandas.read_csv_to_df(stream, **kwargs)
            # Filter by dates if specified.
            if start_ts:
                start_ts = hdateti.convert_timestamp_to_unix_epoch(start_ts)
                data = data[data["timestamp"] >= start_ts]
            if end_ts:
                end_ts = hdateti.convert_timestamp_to_unix_epoch(end_ts)
                data = data[data["timestamp"] <= end_ts]
        else:
            raise ValueError(
                f"Unsupported extension {self._extension}. "
                f"Supported extensions are: `pq`, `csv`, `csv.gz`"
            )
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
        return file_path


# #############################################################################
# CcxtHistoricalPqByTileClient
# #############################################################################


class CcxtHistoricalPqByTileClient(icdc.HistoricalPqByTileClient):
    """
    Read historical data for `CCXT` assets stored as Parquet dataset.

    It can read data from local or S3 filesystem as backend.
    """

    def __init__(
        self,
        resample_1min: bool,
        root_dir: str,
        partition_mode: str,
        *,
        data_snapshot: str = "latest",
        aws_profile: Optional[str] = None,
    ) -> None:
        """
        Constructor.

        See the parent class for parameters description.

        :param data_snapshot: data snapshot at a particular time point, e.g., "20220210"
        """
        vendor = "CCXT"
        infer_exchange_id = True
        super().__init__(
            vendor,
            resample_1min,
            root_dir,
            partition_mode,
            infer_exchange_id,
            aws_profile=aws_profile,
        )
        self._data_snapshot = data_snapshot

    def get_metadata(self) -> pd.DataFrame:
        """
        See description in the parent class.
        """
        raise NotImplementedError

    def get_universe(self) -> List[icdc.FullSymbol]:
        """
        See description in the parent class.
        """
        universe = icunv.get_vendor_universe(
            vendor=self._vendor, as_full_symbol=True
        )
        return universe  # type: ignore[no-any-return]

    @staticmethod
    def _get_columns_for_query() -> List[str]:
        """
        See description in the parent class.
        """
        columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "currency_pair",
        ]
        return columns

    @staticmethod
    def _apply_transformations(
        df: pd.DataFrame, full_symbol_col_name: str, **kwargs
    ) -> pd.DataFrame:
        """
        See description in the parent class.
        """
        if "exchange_id" in kwargs:
            df["exchange_id"] = kwargs["exchange_id"]
        # Convert to string, see the parent class for details.
        df["exchange_id"] = df["exchange_id"].astype(str)
        df["currency_pair"] = df["currency_pair"].astype(str)
        # Add full symbol column.
        df[full_symbol_col_name] = df.apply(
            lambda x: icdc.build_full_symbol(
                x["exchange_id"], x["currency_pair"]
            ),
            axis=1,
        )
        # Select only necessary columns.
        columns = [full_symbol_col_name, "open", "high", "low", "close", "volume"]
        df = df[columns]
        return df

    def _get_root_dirs_symbol_filters(
        self, full_symbols: List[icdc.FullSymbol], full_symbol_col_name: str
    ) -> Dict[str, hparque.ParquetFilter]:
        """
        Build a dict with exchange root dirs of the `CCXT` data as keys and
        filtering conditions on corresponding currency pairs as values.

        E.g.,
        ```
        {
            "s3://cryptokaizen-data/historical/ccxt/latest/binance": (
                "currency_pair", "in", ["ADA_USDT", "BTC_USDT"]
            ),
            "s3://cryptokaizen-data/historical/ccxt/latest/coinbase": (
                "currency_pair", "in", ["BTC_USDT", "ETH_USDT"]
            ),
        }
        ```
        """
        # Build a root dir to the list of exchange ids subdirs, e.g.,
        # "s3://cryptokaizen-data/historical/ccxt/latest/binance".
        root_dir = os.path.join(
            self._root_dir, self._vendor.lower(), self._data_snapshot
        )
        # Split full symbols into exchange id and currency pair tuples, e.g.,
        # [('binance', 'ADA_USDT'),
        # ('coinbase', 'BTC_USDT')].
        full_symbol_tuples = [
            icdc.parse_full_symbol(full_symbol) for full_symbol in full_symbols
        ]
        # Store full symbols as a dictionary, e.g., `{exchange_id1: [currency_pair1, currency_pair2]}`.
        # `Defaultdict` provides a default value for the key that does not exists that prevents from
        # getting `KeyError`.
        symbol_dict = collections.defaultdict(list)
        for exchange_id, *currency_pair in full_symbol_tuples:
            symbol_dict[exchange_id].extend(currency_pair)
        # Build a dict with exchange root dirs as keys and Parquet filters by
        # the corresponding currency pairs as values.
        root_dir_symbol_filter_dict = {
            os.path.join(root_dir, exchange_id): (
                "currency_pair",
                "in",
                currency_pairs,
            )
            for exchange_id, currency_pairs in symbol_dict.items()
        }
        return root_dir_symbol_filter_dict
