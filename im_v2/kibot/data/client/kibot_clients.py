"""
Import as:

import im_v2.kibot.data.client.kibot_clients as imvkdckicl
"""

import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd

import core.pandas_helpers as cpanh
import helpers.hdbg as hdbg
import helpers.hs3 as hs3
import im_v2.common.data.client as icdc

_LOG = logging.getLogger(__name__)


# #############################################################################
# KibotEquitiesCsvParquetByAssetClient
# #############################################################################


class KibotEquitiesCsvParquetByAssetClient(icdc.ImClientReadingOneSymbol):
    def __init__(
        self,
        root_dir: str,
        extension: str,
        asset_class: str,
        unadjusted: Optional[bool],
        *,
        aws_profile: Optional[str] = None,
    ) -> None:
        """
        Constructor.
        """
        vendor = "kibot"
        super().__init__(vendor)
        self._root_dir = root_dir
        # Verify that extension does not start with "." and set parameter.
        hdbg.dassert(
            not extension.startswith("."),
            "The extension %s should not start with '.'",
            extension,
        )
        self._extension = extension
        #
        self._asset_class = asset_class
        _asset_classes = ["etfs", "stocks", "forex", "sp_500"]
        hdbg.dassert_in(self._asset_class, _asset_classes)
        #
        if unadjusted is None:
            hdbg.dassert_is(
                self._asset_class,
                "forex",
                msg="`unadjusted` is a required arg for asset "
                "classes: 'stocks' & 'etfs' & 'sp_500'",
            )
            self._unadjusted = False
        else:
            self._unadjusted = unadjusted
        # Set s3fs parameter value if aws profile parameter is specified.
        if aws_profile:
            self._s3fs = hs3.get_s3fs(aws_profile)

    def get_universe(self) -> List[icdc.FullSymbol]:
        """
        See description in the parent class.
        """
        # TODO(Dan): Find a way to get all Kibot equities universe.
        #  Return `[]` to prevent code from break.
        return []

    def _read_data_for_one_symbol(
        self,
        full_symbol: icdc.FullSymbol,
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        **kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Read Kibot data.
        """
        # TODO(Dan): Do we need `exchange` param here? If so, how to use it?
        # Split full symbol into exchange and trade symbol.
        exchange_id, trade_symbol = icdc.parse_full_symbol(full_symbol)
        # Get absolute file path for a file with equities data.
        file_path = self._get_file_path(trade_symbol)
        # Read raw equities data.
        # TODO(Dan): Should we add `unadjusted` to this log somehow?
        _LOG.info(
            "Reading data for Kibot, exchange id='%s', asset class='%s', "
            "trade symbol='%s' from file='%s'...",
            exchange_id,
            self._asset_class,
            trade_symbol,
            file_path,
        )
        if hs3.is_s3_path(file_path):
            # Add s3fs argument to kwargs.
            kwargs["s3fs"] = self._s3fs
        # Read data.
        if self._extension == "pq":
            # Initialize list of filters.
            filters = []
            if start_ts:
                # Add filtering by start timestamp if specified.
                filters.append(("datetime", ">=", start_ts.tz_localize(None)))
            if end_ts:
                # Add filtering by end timestamp if specified.
                filters.append(("datetime", "<=", end_ts.tz_localize(None)))
            if filters:
                # Add filters to kwargs if any were set.
                kwargs["filters"] = filters
            # Load and normalize data.
            data = cpanh.read_parquet(file_path, **kwargs)
            data = self._apply_kibot_parquet_normalization(data)
        elif self._extension in ["csv", "csv.gz"]:
            kwargs["header"] = None
            kwargs["names"] = [
                "date",
                "time",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]
            # Load and normalize data.
            data = cpanh.read_csv(file_path, **kwargs)
            data = self._apply_kibot_csv_normalization(data)
            # Filter by dates if specified.
            if start_ts:
                data = data[data.index >= start_ts]
            if end_ts:
                data = data[data.index <= end_ts]
        else:
            raise ValueError(
                f"Unsupported extension {self._extension}. "
                f"Supported extensions are: `pq`, `csv`, `csv.gz`"
            )
        return data

    def _get_file_path(
        self,
        trade_symbol: str,
    ) -> str:
        """
        Get the absolute path to a file with Kibot equities data.
        """
        # Get absolute file path.
        file_name = ".".join([trade_symbol, self._extension])
        pq_subdir = ""
        if self._extension == "pq":
            pq_subdir = "pq"
        file_path = os.path.join(
            self._root_dir,
            self._vendor,
            pq_subdir,
            self._get_subdir_name(),
            file_name,
        )
        # TODO(Dan): Remove asserts below after CMTask108 is resolved.
        # Verify that the file exists.
        if hs3.is_s3_path(file_path):
            hs3.dassert_s3_exists(file_path, self._s3fs)
        else:
            hdbg.dassert_file_exists(file_path)
        return file_path

    def _get_subdir_name(self) -> str:
        """
        Get subdir name where Kibot data is stored.
        """
        _asset_class_prefix_mapping = {
            "etfs": "all_etfs",
            "stocks": "all_stocks",
            "forex": "all_forex_pairs",
            "sp_500": "sp_500",
        }
        subdir_name = _asset_class_prefix_mapping[self._asset_class]
        if self._unadjusted:
            subdir_name = "_".join([subdir_name, "unadjusted"])
        subdir_name = "_".join([subdir_name, "1min"])
        return subdir_name

    @staticmethod
    def _apply_kibot_csv_normalization(data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformations to Kibot data in CSV format.
        """
        timestamp_column = pd.to_datetime(
            data["date"] + " " + data["time"], utc=True
        )
        data = data.set_index(timestamp_column)
        data = data.drop(["time", "date"], axis=1)
        return data

    @staticmethod
    def _apply_kibot_parquet_normalization(data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformations to Kibot data in Parquet format.
        """
        data.index.name = None
        data.index = data.index.tz_localize("utc")
        data = data.rename(columns={"vol": "volume"})
        return data


# #############################################################################
# KibotFuturesCsvParquetByAssetClient
# #############################################################################


class KibotFuturesCsvParquetByAssetClient(icdc.ImClientReadingOneSymbol):
    def __init__(
        self,
        root_dir: str,
        extension: str,
        contract_type: bool,
        *,
        aws_profile: Optional[str] = None,
    ) -> None:
        """
        Constructor.
        """
        vendor = "kibot"
        super().__init__(vendor)
        self._root_dir = root_dir
        # Verify that extension does not start with "." and set parameter.
        hdbg.dassert(
            not extension.startswith("."),
            "The extension %s should not start with '.'",
            extension,
        )
        self._extension = extension
        #
        self._contract_type = contract_type
        _contract_types = ["continuous", "expiry"]
        hdbg.dassert_in(self._contract_type, _contract_types)
        #
        # Set s3fs parameter value if aws profile parameter is specified.
        if aws_profile:
            self._s3fs = hs3.get_s3fs(aws_profile)

    def get_universe(self) -> List[icdc.FullSymbol]:
        """
        See description in the parent class.
        """
        # TODO(Dan): Find a way to get all Kibot futures universe.
        #  Return `[]` to prevent code from break.
        return []

    def _read_data_for_one_symbol(
        self,
        full_symbol: icdc.FullSymbol,
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        **kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Read Kibot data.
        """
        # TODO(Dan): Do we need `exchange` param here? If so, how to use it?
        # Split full symbol into exchange and trade symbol.
        exchange_id, trade_symbol = icdc.parse_full_symbol(full_symbol)
        # Get absolute file path for a file with futures data.
        file_path = self._get_file_path(trade_symbol)
        # Read raw futures data.
        # TODO(Dan): Should we add `contract_type` to this log somehow?
        _LOG.info(
            "Reading data for Kibot futures, exchange id='%s', trade symbol='%s' "
            "from file='%s'...",
            exchange_id,
            trade_symbol,
            file_path,
        )
        if hs3.is_s3_path(file_path):
            # Add s3fs argument to kwargs.
            kwargs["s3fs"] = self._s3fs
        # Read data.
        if self._extension == "pq":
            # Initialize list of filters.
            filters = []
            if start_ts:
                # Add filtering by start timestamp if specified.
                filters.append(("datetime", ">=", start_ts.tz_localize(None)))
            if end_ts:
                # Add filtering by end timestamp if specified.
                filters.append(("datetime", "<=", end_ts.tz_localize(None)))
            if filters:
                # Add filters to kwargs if any were set.
                kwargs["filters"] = filters
            # Load and normalize data.
            data = cpanh.read_parquet(file_path, **kwargs)
            data = self._apply_kibot_parquet_normalization(data)
        elif self._extension in ["csv", "csv.gz"]:
            kwargs["header"] = None
            kwargs["names"] = [
                "date",
                "time",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]
            # Load and normalize data.
            data = cpanh.read_csv(file_path, **kwargs)
            data = self._apply_kibot_csv_normalization(data)
            # Filter by dates if specified.
            if start_ts:
                data = data[data.index >= start_ts]
            if end_ts:
                data = data[data.index <= end_ts]
        else:
            raise ValueError(
                f"Unsupported extension {self._extension}. "
                f"Supported extensions are: `pq`, `csv`, `csv.gz`"
            )
        return data

    def _get_file_path(self, trade_symbol: str) -> str:
        """
        Get the absolute path to a file with Kibot futures data.
        """
        # Get absolute file path.
        file_name = ".".join([trade_symbol, self._extension])
        subdir = self._get_subdir_name()
        pq_subdir = ""
        if self._extension == "pq":
            pq_subdir = "pq"
            subdir = "_".join([e.capitalize() for e in subdir.split("_")])
        file_path = os.path.join(
            self._root_dir,
            self._vendor,
            pq_subdir,
            subdir,
            file_name,
        )
        # TODO(Dan): Remove asserts below after CMTask108 is resolved.
        # Verify that the file exists.
        if hs3.is_s3_path(file_path):
            hs3.dassert_s3_exists(file_path, self._s3fs)
        else:
            hdbg.dassert_file_exists(file_path)
        return file_path

    def _get_subdir_name(self) -> str:
        """
        Get subdir name where Kibot data is stored.
        """
        subdir_name = "all_futures"
        if self._contract_type == "continuous":
            subdir_name = "_".join([subdir_name, "continuous", "contracts"])
        subdir_name = "_".join([subdir_name, "1min"])
        return subdir_name

    @staticmethod
    def _apply_kibot_csv_normalization(data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformations to Kibot data in CSV format.
        """
        timestamp_column = pd.to_datetime(
            data["date"] + " " + data["time"], utc=True
        )
        data = data.set_index(timestamp_column)
        data = data.drop(["time", "date"], axis=1)
        return data

    @staticmethod
    def _apply_kibot_parquet_normalization(data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformations to Kibot data in Parquet format.
        """
        data.index.name = None
        data.index = data.index.tz_localize("utc")
        data = data.rename(columns={"vol": "volume"})
        return data
