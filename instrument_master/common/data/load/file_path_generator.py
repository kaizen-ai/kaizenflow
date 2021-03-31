"""
Import as:

import instrument_master.common.data.load.file_path_generator as vcdlfi
"""
import abc
from typing import Optional

import instrument_master.common.data.types as vcdtyp


# TODO(*): -> S3FilePathGenerator
class FilePathGenerator(abc.ABC):
    """
    Generates path from specific security to the file storing the data on S3.
    """

    @abc.abstractmethod
    def generate_file_path(
        self,
        symbol: str,
        frequency: vcdtyp.Frequency,
        # TODO(*): Let's remove this default.
        asset_class: vcdtyp.AssetClass = vcdtyp.AssetClass.Futures,
        contract_type: Optional[vcdtyp.ContractType] = None,
        unadjusted: Optional[bool] = None,
        # TODO(*): Is this needed?
        ext: vcdtyp.Extension = vcdtyp.Extension.Parquet,
    ) -> str:
        """
        Get the path to a specific symbol on s3.

        Parameters as in `read_data`.
        :return: path to the file
        """
