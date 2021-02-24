import abc
from typing import Optional

import vendors_amp.common.data.types as vcdtyp


class FilePathGenerator(abc.ABC):
    @abc.abstractmethod
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
        Get the path to a specific symbol on s3.

        Parameters as in `read_data`.
        :return: path to the file
        """
