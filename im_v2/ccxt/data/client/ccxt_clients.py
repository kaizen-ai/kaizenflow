"""
Import as:

import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl
"""

import abc
import logging
import os
from typing import Any, Optional

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.common.data.client as icdc
import im_v2.common.data_snapshot as icdds
import im_v2.common.universe as ivcu

_LOG = logging.getLogger(__name__)

# #############################################################################
# CcxtCddClient
# #############################################################################


# TODO(gp): It would be better to plug the functions directly in the classes,
#  rather than complicating the class hierarchy. Also it seems that there is
#  a single derived class.
class CcxtCddClient(icdc.ImClient, abc.ABC):
    """
    Contain common code for all the `CCXT` and `CDD` clients.

    E.g.,
    - getting `CCXT` and `CDD` universe
    - applying common transformation for all the data from `CCXT` and `CDD`
        - E.g., `_apply_olhlcv_transformations()`,
          `_apply_vendor_normalization()`
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
            # Column name for `volume` depends on the `currency_pair`, e.g.,
            # there are 2 columns `Volume BTC` and `Volume USDT` for `currency
            # pair `BTC_USDT. And there is no easy way to select the right
            # `Volume` column without passing `currency_pair` that will
            # complicate the interface. To get rid of this dependency the
            # column's index is used.
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
        # Round up float values in case values in raw data are rounded up
        # incorrectly when being read from a file.
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
    ) -> None:
        vendor = "ccxt"
        super().__init__(vendor, resample_1min, db_connection, table_name)

    @staticmethod
    def should_be_online() -> bool:  # pylint: disable=arguments-differ'
        """
        The real-time system for CCXT should always be online.
        """
        return True


# #############################################################################
# CcxtCddCsvParquetByAssetClient
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
        data_snapshot: str,
        *,
        aws_profile: Optional[str] = None,
    ) -> None:
        """
        Load `CCXT` data from local or S3 filesystem.

        :param vendor: price data provider, i.e. `CCXT` or `CDD`
        :param root_dir: either a local root path (e.g., `/app/im`) or
            an S3 root path (e.g., `s3://<ck-data>/reorg/historical.manual.pq`) to `CCXT` data
        :param extension: file extension, e.g., `csv.gz` or `parquet`
        :param aws_profile: AWS profile, e.g., `am`
        :param data_snapshot: same format used in `get_data_snapshot()`
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
        data_snapshot = icdds.get_data_snapshot(
            root_dir, data_snapshot, aws_profile
        )
        self._data_snapshot = data_snapshot
        # Set s3fs parameter value if aws profile parameter is specified.
        if aws_profile:
            self._s3fs = hs3.get_s3fs(aws_profile)
        # TODO(Sonya): Consider moving it to the base class as the `dataset` param.
        self._dataset = "ohlcv"

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
        `<root_dir>/<data_snapshot>/<dataset>/<vendor>/<exchange_id>/<currency_pair>.<extension>`.

        E.g., `s3://.../20210924/ohlcv/ccxt/binance/BTC_USDT.csv.gz`.

        :param data_snapshot: same format used in `get_data_snapshot()`
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


class CcxtHistoricalPqByTileClient(icdc.HistoricalPqByCurrencyPairTileClient):
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
        dataset: str,
        contract_type: str,
        data_snapshot: str,
        *,
        aws_profile: Optional[str] = None,
    ) -> None:
        """
        Constructor.

        See the parent class for parameters description.
        """
        vendor = "CCXT"
        super().__init__(
            vendor,
            universe_version,
            resample_1min,
            root_dir,
            partition_mode,
            dataset,
            contract_type,
            data_snapshot,
            aws_profile=aws_profile,
        )
