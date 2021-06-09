"""
Import as:

import im.kibot.data.load.kibot_file_path_generator as kfpgen
"""

# TODO(*): -> kibot_file_path_generator.py

import os
from typing import Optional, cast

import helpers.dbg as dbg
import im.common.data.load.file_path_generator as icdlfi
import im.common.data.types as icdtyp
import im.kibot.data.config as ikdcon


class KibotFilePathGenerator(icdlfi.FilePathGenerator):
    FREQ_PATH_MAPPING = {
        icdtyp.Frequency.Daily: "daily",
        icdtyp.Frequency.Minutely: "1min",
        icdtyp.Frequency.Tick: "tick",
    }

    CONTRACT_PATH_MAPPING = {
        icdtyp.ContractType.Continuous: "continuous_",
        icdtyp.ContractType.Expiry: "",
    }

    ASSET_TYPE_PREFIX = {
        icdtyp.AssetClass.ETFs: "all_etfs_",
        icdtyp.AssetClass.Stocks: "all_stocks_",
        icdtyp.AssetClass.Forex: "all_forex_pairs_",
        icdtyp.AssetClass.Futures: "all_futures",
        icdtyp.AssetClass.SP500: "sp_500_",
    }

    def generate_file_path(
        self,
        symbol: str,
        frequency: icdtyp.Frequency,
        asset_class: icdtyp.AssetClass = icdtyp.AssetClass.Futures,
        contract_type: Optional[icdtyp.ContractType] = None,
        exchange: Optional[str] = None,
        currency: Optional[str] = None,
        unadjusted: Optional[bool] = None,
        ext: icdtyp.Extension = icdtyp.Extension.Parquet,
    ) -> str:
        """
        Get the path to a specific Kibot dataset on S3.

        Parameters as in `read_data()`.

        :return: path to the file
        """
        freq_path = self.FREQ_PATH_MAPPING[frequency]
        asset_class_prefix = self.ASSET_TYPE_PREFIX[asset_class]
        modifier = self._generate_modifier(
            asset_class=asset_class,
            contract_type=contract_type,
            unadjusted=unadjusted,
        )
        dir_name = f"{asset_class_prefix}{modifier}{freq_path}"
        file_path = os.path.join(dir_name, symbol)
        if ext == icdtyp.Extension.Parquet:
            # Parquet files are located in `pq/` subdirectory.
            file_path = os.path.join("pq", file_path)
            file_path += ".pq"
        elif ext == icdtyp.Extension.CSV:
            file_path += ".csv.gz"
        # TODO(amr): should we allow pointing to a local file here?
        # or rename the method to `generate_s3_path`?
        file_path = os.path.join(ikdcon.S3_PREFIX, file_path)
        return file_path

    @staticmethod
    def get_latest_symbols_file() -> str:
        """
        Get the latest available file with symbols.
        """
        raise NotImplementedError

    def _generate_contract_path_modifier(
        self, contract_type: icdtyp.ContractType
    ) -> str:
        contract_path = self.CONTRACT_PATH_MAPPING[contract_type]
        return f"_{contract_path}contracts_"

    @staticmethod
    def _generate_unadjusted_modifier(unadjusted: bool) -> str:
        adjusted_modifier = "unadjusted_" if unadjusted else ""
        return adjusted_modifier

    def _generate_modifier(
        self,
        asset_class: icdtyp.AssetClass,
        unadjusted: Optional[bool] = None,
        contract_type: Optional[icdtyp.ContractType] = None,
    ) -> str:
        """
        Generate a modifier to the file path, based on some asset class
        options.

        :param asset_class: asset class
        :param unadjusted: required for asset classes of type: `stocks` & `etfs`
        :param contract_type: required for asset class of type: `futures`
        :return: a path modifier
        """
        modifier = ""
        if asset_class == icdtyp.AssetClass.Futures:
            dbg.dassert_is_not(
                contract_type,
                None,
                msg="`contract_type` is a required arg for asset class: 'futures'",
            )
            modifier = self._generate_contract_path_modifier(
                contract_type=contract_type
            )
        elif asset_class in [
            icdtyp.AssetClass.Stocks,
            icdtyp.AssetClass.ETFs,
            icdtyp.AssetClass.SP500,
        ]:
            dbg.dassert_is_not(
                unadjusted,
                None,
                msg="`unadjusted` is a required arg for asset "
                "classes: 'stocks' & 'etfs' & 'sp_500'",
            )
            modifier = self._generate_unadjusted_modifier(
                unadjusted=cast(bool, unadjusted)
            )
        return modifier
