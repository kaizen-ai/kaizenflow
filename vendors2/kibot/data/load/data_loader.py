from typing import Optional

import pandas as pd

import helpers.cache as hcac
import helpers.dbg as dbg
import helpers.s3 as hs3
import vendors2.kibot.data.load.file_path_generator as fpgen
import vendors2.kibot.data.transform.normalizers as nls
import vendors2.kibot.data.types as types


class KibotDataLoader:
    @staticmethod
    @hcac.cache
    def read_data(
        symbol: str,
        asset_class: types.AssetClass,
        frequency: types.Frequency,
        contract_type: Optional[types.ContractType] = None,
        unadjusted: Optional[bool] = None,
        nrows: Optional[int] = None,
    ) -> pd.DataFrame:
        """Read kibot data.

        :param symbol: symbol to get the data for
        :param asset_class: asset class
        :param frequency: `D` or `T` for daily or minutely data respectively
        :param contract_type: required for asset class of type: `futures`
        :param unadjusted: required for asset classes of type: `stocks` & `etfs`
        :param nrows: if not None, return only the first nrows of the data
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

    @staticmethod
    def _read_data(
        symbol: str,
        asset_class: types.AssetClass,
        frequency: types.Frequency,
        contract_type: Optional[types.ContractType] = None,
        unadjusted: Optional[bool] = None,
        nrows: Optional[int] = None,
    ) -> pd.DataFrame:

        file_path = fpgen.FilePathGenerator().generate_file_path(
            symbol=symbol,
            asset_class=asset_class,
            frequency=frequency,
            contract_type=contract_type,
            unadjusted=unadjusted,
            ext=types.Extension.CSV,
        )

        if hs3.is_s3_path(file_path):
            dbg.dassert_is(
                hs3.exists(file_path), True, msg=f"S3 key not found: {file_path}"
            )

        df = pd.read_csv(file_path, header=None, nrows=nrows)

        df = nls.normalize(df=df, frequency=frequency)

        return df
