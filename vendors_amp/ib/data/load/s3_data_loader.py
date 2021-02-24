"""
Load S3 data for IB provider.

Import as:

vendors_amp.ib.data.load.s3_data_loader ibs3
"""

import functools
import logging
from typing import Optional

import pandas as pd

import helpers.dbg as dbg
import helpers.s3 as hs3
import vendors_amp.common.data.load.s3_data_loader as vcdls3
import vendors_amp.common.data.types as vcdtyp
import vendors_amp.ib.data.load.file_path_generator as ibf

_LOG = logging.getLogger(__name__)


class S3IbDataLoader(vcdls3.AbstractS3DataLoader):
    S3_COLUMNS = {
        "date": "datetime64[ns]",
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": int,
        "average": float,
        "barCount": int,
    }

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
        # Find path to look for a data.
        file_path = ibf.IbFilePathGenerator().generate_file_path(
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
        data = pd.read_csv(file_path, nrows=nrows)
        # Arrange types.
        data = data.astype(cls.S3_COLUMNS)
        return data
