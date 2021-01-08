from typing import Dict, Tuple

import helpers.dbg as dbg
import vendors2.kibot.data.load.file_path_generator as vkdlfi
import vendors2.kibot.data.types as vkdtyp


class DatasetNameParser:

    FREQ_PATH_MAPPING: Dict[str, vkdtyp.Frequency] = {
        v: k for k, v in vkdlfi.FilePathGenerator.FREQ_PATH_MAPPING.items()
    }

    CONTRACT_PATH_MAPPING = {
        v: k for k, v in vkdlfi.FilePathGenerator.CONTRACT_PATH_MAPPING.items()
    }

    ASSET_TYPE_PREFIX = {
        v: k for k, v in vkdlfi.FilePathGenerator.ASSET_TYPE_PREFIX.items()
    }

    def parse_dataset_name(
        self,
        dataset: str,
    ) -> Tuple[vkdtyp.AssetClass, vkdtyp.ContractType, vkdtyp.Frequency, bool]:
        asset_class = self._detect_asset_class(dataset)
        contract_type = self._detect_contract_type(dataset)
        frequency = self._detect_frequency(dataset)
        unadjusted = False
        return asset_class, contract_type, frequency, unadjusted

    def _detect_frequency(self, dataset: str) -> vkdtyp.Frequency:
        frequency = None
        for string, _frequency in self.FREQ_PATH_MAPPING.items():
            if dataset.endswith(string):
                frequency = _frequency
        if frequency is None:
            dbg.dfatal(f"${dataset} does not contain frequency.")
        return frequency

    def _detect_asset_class(self, dataset: str) -> vkdtyp.AssetClass:
        asset_class = None
        for string, _asset_class in self.ASSET_TYPE_PREFIX.items():
            if dataset.startswith(string):
                asset_class = _asset_class
        if asset_class is None:
            dbg.dfatal(f"${dataset} does not contain asset class.")
        return asset_class

    def _detect_contract_type(self, dataset: str) -> vkdtyp.ContractType:
        contract_type = None
        for string, _contract_type in self.CONTRACT_PATH_MAPPING.items():
            if len(string) == 0:
                # Skin Expiry contract type, it is the default.
                continue
            if dataset.startswith(string):
                contract_type = _contract_type
        if contract_type is None:
            return vkdtyp.ContractType.Expiry
        return contract_type
