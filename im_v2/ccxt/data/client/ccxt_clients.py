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
import im_v2.common.universe as ivcu

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

    def __init__(
        self, vendor: str, universe_version: str, resample_1min: bool
    ) -> None:
        """
        Constructor.
        """
        super().__init__(vendor, universe_version, resample_1min)
        _vendors = ["CCXT", "CDD"]
        hdbg.dassert_in(self._vendor, _vendors)

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
        # Round up float values in case values in raw data are rounded up incorrectly when
        # being read from a file.
        data = data.round(8)
        return data


# #############################################################################
# CcxtSqlRealTimeImClient
# #############################################################################


class CcxtSqlRealTimeImClient(icdc.SqlRealTimeImClient):
    def __init__(
        self,
        resample_1min: bool,
        db_connection: hsql.DbConnection,
        table_name: str,
        mode: str = "data_client",
    ) -> None:
        super().__init__(resample_1min, db_connection, table_name, vendor="ccxt")
        self._mode = mode

    @staticmethod
    def should_be_online() -> bool:  # pylint: disable=arguments-differ'
        """
        The real-time system for CCXT should always be online.
        """
        return True

    def _apply_normalization(
        self,
        data: pd.DataFrame,
        *,
        full_symbol_col_name: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Apply Talos-specific normalization.

         `data_client` mode:
        - Convert `timestamp` column to a UTC timestamp and set index.
        - Keep `open`, `high`, `low`, `close`, `volume` columns.

        `market_data` mode:
        - Add `start_timestamp` column in UTC timestamp format.
        - Add `end_timestamp` column in UTC timestamp format.
        - Add `asset_id` column which is result of mapping full_symbol to integer.
        - Drop extra columns.
        - The output looks like:
        ```
        open  high  low   close volume  start_timestamp          end_timestamp            asset_id
        0.825 0.826 0.825 0.825 18427.9 2022-03-16 2:46:00+00:00 2022-03-16 2:47:00+00:00 3303714233
        0.825 0.826 0.825 0.825 52798.5 2022-03-16 2:47:00+00:00 2022-03-16 2:48:00+00:00 3303714233
        ```
        """
        # Convert timestamp column with Unix epoch to timestamp format.
        data["timestamp"] = data["timestamp"].apply(
            hdateti.convert_unix_epoch_to_timestamp
        )
        full_symbol_col_name = self._get_full_symbol_col_name(
            full_symbol_col_name
        )
        ohlcv_columns = [
            full_symbol_col_name,
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        if self._mode == "data_client":
            pass
        elif self._mode == "market_data":
            # TODO(Danya): Move this transformation to MarketData.
            # Add `asset_id` column using mapping on `full_symbol` column.
            data["asset_id"] = data[full_symbol_col_name].apply(
                ivcu.string_to_numerical_id
            )
            # Convert to int64 to keep NaNs alongside with int values.
            data["asset_id"] = data["asset_id"].astype(pd.Int64Dtype())
            # Generate `start_timestamp` from `end_timestamp` by substracting delta.
            delta = pd.Timedelta("1M")
            data["start_timestamp"] = data["timestamp"].apply(
                lambda pd_timestamp: (pd_timestamp - delta)
            )
            # Columns that should left in the table.
            market_data_ohlcv_columns = [
                "start_timestamp",
                "asset_id",
            ]
            # Concatenate two lists of columns.
            ohlcv_columns = ohlcv_columns + market_data_ohlcv_columns
        else:
            hdbg.dfatal(
                "Invalid mode='%s'. Correct modes: 'market_data', 'data_client'"
                % self._mode
            )
        data = data.set_index("timestamp")
        # Verify that dataframe contains OHLCV columns.
        hdbg.dassert_is_subset(ohlcv_columns, data.columns)
        # Rearrange the columns.
        data = data.loc[:, ohlcv_columns]
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
        universe_version: str,
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
            an S3 root path (e.g., "s3://<ck-data>/reorg/historical.manual.pq") to `CCXT` data
        :param extension: file extension, e.g., `.csv`, `.csv.gz` or `.parquet`
        :param aws_profile: AWS profile name (e.g., "am")
        :param data_snapshot: snapshot of datetime when data was loaded,
            e.g. "20210924"
        """
        super().__init__(vendor, universe_version, resample_1min)
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
        full_symbol: ivcu.FullSymbol,
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        See description in the parent class.
        """
        # Split full symbol into exchange and currency pair.
        exchange_id, currency_pair = ivcu.parse_full_symbol(full_symbol)
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
        `<root_dir>/<snapshot>/<dataset>/<vendor>/<exchange_id>/<currency_pair>.<extension>`
        E.g., `s3://.../20210924/ohlcv/ccxt/binance/BTC_USDT.csv.gz`

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
            data_snapshot,
            self._dataset,
            self._vendor.lower(),
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
        universe_version: str,
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
            universe_version,
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

    @staticmethod
    def _get_columns_for_query(
        full_symbol_col_name: str, columns: Optional[List[str]]
    ) -> Optional[List[str]]:
        """
        See description in the parent class.
        """
        if columns is not None:
            # In order not to modify the input.
            query_columns = columns.copy()
            # Data is partitioned by currency pairs so the column is mandatory.
            query_columns.append("currency_pair")
            # Full symbol column is added after we read data, so skipping here.
            if full_symbol_col_name in query_columns:
                query_columns.remove(full_symbol_col_name)
        else:
            query_columns = columns
        return query_columns

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
        # Add full symbol column at first position in data.
        full_symbol_col = ivcu.build_full_symbol(
            df["exchange_id"], df["currency_pair"]
        )
        if df.columns[0] != full_symbol_col_name:
            # Insert if it doesn't already exist.
            df.insert(0, full_symbol_col_name, full_symbol_col)
        else:
            # Replace the values to ensure the correct data.
            df[full_symbol_col_name] = full_symbol_col.values
        # The columns are used just to partition the data but these columns
        # are not included in the `ImClient` output.
        df = df.drop(["exchange_id", "currency_pair"], axis=1)
        # Round up float values in case values in raw data are rounded up incorrectly when
        # being read from a file.
        df = df.round(8)
        return df

    def _get_root_dirs_symbol_filters(
        self, full_symbols: List[ivcu.FullSymbol], full_symbol_col_name: str
    ) -> Dict[str, hparque.ParquetFilter]:
        """
        Build a dict with exchange root dirs of the `CCXT` data as keys and
        filtering conditions on corresponding currency pairs as values.

        E.g.,
        ```
        {
            "s3://.../20210924/ohlcv/ccxt/binance": (
                "currency_pair", "in", ["ADA_USDT", "BTC_USDT"]
            ),
            "s3://.../20210924/ohlcv/ccxt/coinbase": (
                "currency_pair", "in", ["BTC_USDT", "ETH_USDT"]
            ),
        }
        ```
        """
        root_dir = os.path.join(
            self._root_dir,
            self._data_snapshot,
            self._dataset,
            self._vendor.lower(),
        )
        # Split full symbols into exchange id and currency pair tuples, e.g.,
        # [('binance', 'ADA_USDT'),
        # ('coinbase', 'BTC_USDT')].
        full_symbol_tuples = [
            ivcu.parse_full_symbol(full_symbol) for full_symbol in full_symbols
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
