"""
Import as:

import im_v2.talos.data.client.talos_clients as imvtdctacl
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
import im_v2.common.data.client as icdc

_LOG = logging.getLogger(__name__)


# #############################################################################
# TalosClient
# #############################################################################


class TalosClient(icdc.ImClient, abc.ABC):
    """
    Contain common code for all the `Talos` clients, e.g.,

    - getting `Talos` universe
    - applying common transformation for all the data from `Talos`
    """

    def __init__(self) -> None:
        """
        Constructor.
        """
        vendor = "talos"
        super().__init__(vendor)

    def get_universe(self) -> List[icdc.FullSymbol]:
        """
        See description in the parent class.
        """
        # TODO(Dan): CmTask1318.
        return []

    @staticmethod
    def _apply_talos_normalization(data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformations to `Talos` data.

        Parquet by asset data is normalized to fit parent class output format:
            - index name is dropped
            - only OHLCV columns are selected

        Input data:
        ```
                                   timestamp        open     knowledge_timestamp
        timestamp
        2022-01-01 00:00:00+00:00  1640995200000  102.99 ... 2022-03-09 19:14:50
        2022-01-01 00:01:00+00:00  1640995260000  102.99     2022-03-09 19:14:50
        2022-01-01 00:02:00+00:00  1640995320000  103.18     2022-03-09 19:14:50
        ```
        Output data:
        ```
                                   open       close   volume
        2022-01-01 00:00:00+00:00  102.99 ... 102.99  112
        2022-01-01 00:01:00+00:00  102.99     102.99  112
        2022-01-01 00:02:00+00:00  103.18     103.18  781
        ```
        """
        data.index.name = None
        # Specify OHLCV columns.
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


# #############################################################################
# TalosParquetByAssetClient
# #############################################################################


class TalosParquetByAssetClient(
    TalosClient, icdc.ImClientReadingOneSymbol
):
    """
    Read data from a Parquet file storing data for a single `Talos` asset.

    It can read data from local or S3 filesystem as backend.
    """

    def __init__(
        self,
        root_dir: str,
        *,
        aws_profile: Optional[str] = None,
    ) -> None:
        """
        Load `Talos` data from local or S3 filesystem.

        :param root_dir: either a local root path (e.g., "/app/im") or
            an S3 root path (e.g., "s3://cryptokaizen-data/historical") to `Talos` data
        :param aws_profile: AWS profile name (e.g., "ck")
        """
        super().__init__()
        self._root_dir = root_dir
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
        See the `_read_data_for_one_symbol()` in the parent class.
        """
        # Split full symbol into exchange and currency pair.
        exchange_id, currency_pair = icdc.parse_full_symbol(full_symbol)
        # Get absolute file path for a file with crypto price data.
        file_path = self._get_file_path(exchange_id, currency_pair)
        # Read raw crypto price data.
        _LOG.info(
            "Reading data for `Talos`, exchange id='%s', currencies='%s' from file='%s'...",
            self._vendor,
            exchange_id,
            currency_pair,
            file_path,
        )
        if hs3.is_s3_path(file_path):
            # Add s3fs argument to kwargs.
            kwargs["s3fs"] = self._s3fs
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
        data = cpanh.read_parquet(file_path, **kwargs)
        # Normalize data according to the vendor.
        data = self._apply_vendor_normalization(data)
        return data

    def _get_file_path(
        self,
        exchange_id: str,
        currency_pair: str,
    ) -> str:
        """
        Get the absolute path to a file with `Kibot` price data.

        The file path is constructed in the following way:
        `<root_dir>/<vendor>/<exchange_id>/currency_pair=<currency_pair>`

        :param data_snapshot: snapshot of datetime when data was loaded,
            e.g. "20210924"
        :param exchange_id: exchange id, e.g. "binance"
        :param currency_pair: currency pair `<currency1>_<currency2>`,
            e.g. "BTC_USDT"
        :return: absolute path to a file with `Kibot` price data
        """
        # Get absolute file path.
        file_name = ".".join([currency_pair, ".parquet"])
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
