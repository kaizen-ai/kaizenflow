from typing import Dict, Tuple

import helpers.dbg as dbg
import vendors.kibot.data.load.file_path_generator as vkdlfi
import vendors.kibot.data.types as vkdtyp


class DatasetNameParser:
    """
    Converter of dataset names into enumerated types.
    """

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
        """
        Parse dataset name and return a tuple with types, describing the
        dataset.

        :param dataset: dataset name, e.g. all_futures_contracts_1min
        :return: tuple of asset class, contract type, frequency, and unadjusted
        """
        asset_class = self._extract_asset_class(dataset)
        contract_type = self._extract_contract_type(dataset)
        frequency = self._extract_frequency(dataset)
        unadjusted = False
        return asset_class, contract_type, frequency, unadjusted

    def _extract_frequency(self, dataset: str) -> vkdtyp.Frequency:
        frequency = None
        for string, _frequency in self.FREQ_PATH_MAPPING.items():
            if dataset.endswith(string):
                dbg.dassert_is(
                    frequency,
                    None,
                    "Detected multiple frequencies, e.g., %s, %s",
                    frequency,
                    _frequency,
                )
                frequency = _frequency
        if frequency is None:
            dbg.dfatal(f"${dataset} does not contain frequency.")
        return frequency

    def _extract_asset_class(self, dataset: str) -> vkdtyp.AssetClass:
        asset_class = None
        for string, _asset_class in self.ASSET_TYPE_PREFIX.items():
            if dataset.startswith(string):
                dbg.dassert_is(
                    asset_class,
                    None,
                    "Detected multiple asset classes, e.g., %s, %s",
                    asset_class,
                    _asset_class,
                )
                asset_class = _asset_class
        if asset_class is None:
            dbg.dfatal(f"${dataset} does not contain asset class.")
        return asset_class

    def _extract_contract_type(self, dataset: str) -> vkdtyp.ContractType:
        contract_type = None
        for string, _contract_type in self.CONTRACT_PATH_MAPPING.items():
            if len(string) == 0:
                # Skip Expiry contract type, it is the default.
                continue
            if string in dataset:
                dbg.dassert_is(
                    contract_type,
                    None,
                    "Detected multiple contract types, e.g., %s, %s",
                    contract_type,
                    _contract_type,
                )
                contract_type = _contract_type
        if contract_type is None:
            contract_type = vkdtyp.ContractType.Expiry
        return contract_type
