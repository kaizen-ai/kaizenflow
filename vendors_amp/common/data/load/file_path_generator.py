import os
from typing import Optional, cast

import helpers.dbg as dbg
import vendors_amp.common.data.types as vcdtyp
import vendors_amp.kibot.data.config as vkdcon
import abc

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
