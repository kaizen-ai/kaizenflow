import os
from typing import Optional, cast

import helpers.dbg as dbg
import vendors2.kibot.data.config as config
import vendors2.kibot.data.types as types


class FilePathGenerator:
    FREQ_PATH_MAPPING = {
        types.Frequency.Daily: "daily",
        types.Frequency.Minutely: "1min",
    }

    CONTRACT_PATH_MAPPING = {
        types.ContractType.Continuous: "Continuous_",
        types.ContractType.Expiry: "",
    }

    ASSET_TYPE_PREFIX = {
        types.AssetClass.ETFs: "all_etfs",
        types.AssetClass.Stocks: "all_stocks",
        types.AssetClass.Forex: "all_forex",
        types.AssetClass.Futures: "All_Futures",
    }

    def generate_file_path(
        self,
        symbol: str,
        frequency: types.Frequency,
        asset_class: types.AssetClass = types.AssetClass.Futures,
        contract_type: Optional[types.ContractType] = None,
        unadjusted: Optional[bool] = None,
        ext: types.Extension = types.Extension.Parquet,
    ) -> str:
        """Get the path to a specific kibot dataset on s3.

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

        if ext == types.Extension.Parquet:
            # Parquet files are located in `pq/` subdirectory.
            file_path = os.path.join("pq", file_path)
            file_path += ".pq"
        elif ext == types.Extension.CSV:
            file_path += ".csv.gz"

        # TODO(amr): should we allow pointing to a local file here?
        file_path = os.path.join(config.S3_PREFIX, file_path)
        return file_path

    def _generate_contract_path_modifier(
        self, contract_type: types.ContractType
    ) -> str:
        contract_path = self.CONTRACT_PATH_MAPPING[contract_type]
        return f"_{contract_path}Contracts_"

    @staticmethod
    def _generate_unadjusted_modifier(unadjusted: bool) -> str:
        adjusted_modifier = "_unadjusted_" if unadjusted else ""
        return adjusted_modifier

    def _generate_modifier(
        self,
        asset_class: types.AssetClass,
        unadjusted: Optional[bool] = None,
        contract_type: Optional[types.ContractType] = None,
    ) -> str:
        """Generate a modifier to the file path, based on some asset class
        options.

        :param asset_class: asset class
        :param unadjusted: required for asset classes of type: `stocks` & `etfs`
        :param contract_type: required for asset class of type: `futures`
        :return: a path modifier
        """
        modifier = ""
        if asset_class == types.AssetClass.Futures:
            dbg.dassert_is_not(
                contract_type,
                None,
                msg="`contract_type` is a required arg for asset class: 'futures'",
            )
            modifier = self._generate_contract_path_modifier(
                contract_type=contract_type
            )
        elif asset_class in [types.AssetClass.Stocks, types.AssetClass.ETFs]:
            dbg.dassert_is_not(
                unadjusted,
                None,
                msg="`unadjusted` is a required arg for asset classes: 'stocks' & 'etfs'",
            )
            modifier = self._generate_unadjusted_modifier(
                unadjusted=cast(bool, unadjusted)
            )
        return modifier
