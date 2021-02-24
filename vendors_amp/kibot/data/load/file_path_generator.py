import os
from typing import Optional, cast

import helpers.dbg as dbg
import vendors_amp.common.data.types as vcdtyp
import vendors_amp.kibot.data.config as vkdcon
import vendors_amp.common.data.load.file_path_generator as vcdf

class KibotFilePathGenerator(vcdf.FilePathGenerator):
    FREQ_PATH_MAPPING = {
        vcdtyp.Frequency.Daily: "daily",
        vcdtyp.Frequency.Minutely: "1min",
        vcdtyp.Frequency.Tick: "tick",
    }

    CONTRACT_PATH_MAPPING = {
        vcdtyp.ContractType.Continuous: "continuous_",
        vcdtyp.ContractType.Expiry: "",
    }

    ASSET_TYPE_PREFIX = {
        vcdtyp.AssetClass.ETFs: "all_etfs_",
        vcdtyp.AssetClass.Stocks: "all_stocks_",
        vcdtyp.AssetClass.Forex: "all_forex_pairs_",
        vcdtyp.AssetClass.Futures: "all_futures",
        vcdtyp.AssetClass.SP500: "sp_500_",
    }

    def generate_file_path(
        self,
        symbol: str,
        frequency: vcdtyp.Frequency,
        asset_class: vcdtyp.AssetClass = vcdtyp.AssetClass.Futures,
        contract_type: Optional[vcdtyp.ContractType] = None,
        unadjusted: Optional[bool] = None,
        ext: vcdtyp.Extension = vcdtyp.Extension.Parquet,
    ) -> str:
        """
        Get the path to a specific kibot dataset on s3.

        Parameters as in `read_data`.
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

        if ext == vcdtyp.Extension.Parquet:
            # Parquet files are located in `pq/` subdirectory.
            file_path = os.path.join("pq", file_path)
            file_path += ".pq"
        elif ext == vcdtyp.Extension.CSV:
            file_path += ".csv.gz"

        # TODO(amr): should we allow pointing to a local file here?
        # or rename the method to `generate_s3_path`?
        file_path = os.path.join(vkdcon.S3_PREFIX, file_path)
        return file_path

    def _generate_contract_path_modifier(
        self, contract_type: vcdtyp.ContractType
    ) -> str:
        contract_path = self.CONTRACT_PATH_MAPPING[contract_type]
        return f"_{contract_path}contracts_"

    @staticmethod
    def _generate_unadjusted_modifier(unadjusted: bool) -> str:
        adjusted_modifier = "unadjusted_" if unadjusted else ""
        return adjusted_modifier

    def _generate_modifier(
        self,
        asset_class: vcdtyp.AssetClass,
        unadjusted: Optional[bool] = None,
        contract_type: Optional[vcdtyp.ContractType] = None,
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
        if asset_class == vcdtyp.AssetClass.Futures:
            dbg.dassert_is_not(
                contract_type,
                None,
                msg="`contract_type` is a required arg for asset class: 'futures'",
            )
            modifier = self._generate_contract_path_modifier(
                contract_type=contract_type
            )
        elif asset_class in [
            vcdtyp.AssetClass.Stocks,
            vcdtyp.AssetClass.ETFs,
            vcdtyp.AssetClass.SP500,
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
