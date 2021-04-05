"""
Import as:

instrument_master.ib.data.load.ib_s3_data_loader ibs3
"""

import functools
import logging
from typing import Optional

import pandas as pd

import helpers.dbg as dbg
import helpers.s3 as hs3
import instrument_master.common.data.load.s3_data_loader as vcdls3
import instrument_master.common.data.types as vcdtyp
import instrument_master.ib.data.load.ib_file_path_generator as vidlfi

_LOG = logging.getLogger(__name__)


class IbS3DataLoader(vcdls3.AbstractS3DataLoader):
    """
    Reads IB data from S3.
    """

    S3_COLUMNS = {
        "date": "object",
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": int,
        "average": float,
        "barCount": int,
    }
    S3_DATE_COLUMNS = ["date"]

    # TODO(plyq): Uncomment once #1047 will be resolved.
    # @hcache.cache
    # Use lru_cache for now.
    @functools.lru_cache(maxsize=64)
    def read_data(
        self,
        exchange: str,
        symbol: str,
        asset_class: vcdtyp.AssetClass,
        frequency: vcdtyp.Frequency,
        contract_type: Optional[vcdtyp.ContractType] = None,
        unadjusted: Optional[bool] = None,
        nrows: Optional[int] = None,
        normalize: bool = True,
    ) -> pd.DataFrame:
        """
        Read ib data.

        :param exchange: name of the exchange
        :param symbol: symbol to get the data for
        :param asset_class: asset class
        :param frequency: `D` or `T` for daily or minutely data respectively
        :param contract_type: required for asset class of type: `futures`
        :param unadjusted: required for asset classes of type: `stocks` & `etfs`
        :param nrows: if not None, return only the first nrows of the data
        :param normalize: whether to normalize the dataframe by frequency
        :return: a dataframe with the symbol data
        """
        return self._read_data(
            symbol=symbol,
            asset_class=asset_class,
            frequency=frequency,
            contract_type=contract_type,
            unadjusted=unadjusted,
            nrows=nrows,
        )

    @classmethod
    def _read_data(
        cls,
        symbol: str,
        asset_class: vcdtyp.AssetClass,
        frequency: vcdtyp.Frequency,
        contract_type: Optional[vcdtyp.ContractType] = None,
        unadjusted: Optional[bool] = None,
        nrows: Optional[int] = None,
    ) -> pd.DataFrame:
        # Generate path to retrieve data.
        file_path = vidlfi.IbFilePathGenerator().generate_file_path(
            symbol=symbol,
            asset_class=asset_class,
            frequency=frequency,
            contract_type=contract_type,
            unadjusted=unadjusted,
            ext=vcdtyp.Extension.CSV,
        )
        # Check that file exists.
        if hs3.is_s3_path(file_path):
            dbg.dassert_is(
                hs3.exists(file_path), True, msg=f"S3 key not found: {file_path}"
            )
        # Read data.
        # cls.S3_COLUMNS.keys() -> list(cls.S3_COLUMNS.keys())
        # https://github.com/pandas-dev/pandas/issues/36928 fixed in Pandas 1.1.4
        data = pd.read_csv(
            file_path, nrows=nrows, names=list(cls.S3_COLUMNS.keys())
        )
        # TODO(plyq): Reload ES data with a new extractor to have a header.
        # If header was already in data, remove it.
        if list(data.iloc[0]) == list(cls.S3_COLUMNS.keys()):
            data = data[1:].reset_index(drop=True)
        # Cast columns to correct types.
        data = data.astype(
            {
                key: cls.S3_COLUMNS[key]
                for key in cls.S3_COLUMNS
                if key not in cls.S3_DATE_COLUMNS
            }
        )
        for date_column in cls.S3_DATE_COLUMNS:
            data[date_column] = pd.to_datetime(data[date_column])
        return data
