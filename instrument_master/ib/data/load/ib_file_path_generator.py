import os
from typing import Optional

import instrument_master.common.data.load.file_path_generator as icdlfi
import instrument_master.common.data.types as icdtyp
import instrument_master.ib.data.config as iidcon


class IbFilePathGenerator(icdlfi.FilePathGenerator):
    """
    Generate file path for a symbol.

    File path is: <symbol>_<frequency>.<extension>
    """

    FREQ_PATH_MAPPING = {
        icdtyp.Frequency.Daily: "daily",
        icdtyp.Frequency.Hourly: "hourly",
        icdtyp.Frequency.Minutely: "minutely",
        icdtyp.Frequency.Tick: "tick",
    }

    def generate_file_path(
        self,
        symbol: str,
        frequency: icdtyp.Frequency,
        asset_class: icdtyp.AssetClass = icdtyp.AssetClass.Futures,
        contract_type: Optional[icdtyp.ContractType] = None,
        unadjusted: Optional[bool] = None,
        ext: icdtyp.Extension = icdtyp.Extension.CSV,
    ) -> str:
        """
        Get the path to a specific IB dataset on S3.

        Format like `continuous_futures/daily/ES.csv.gz`.

        Parameters as in `read_data`.
        :return: path to the file
        """
        # Get asset class part.
        if (
            contract_type == icdtyp.ContractType.Continuous
            and asset_class == icdtyp.AssetClass.Futures
        ):
            asset_part = "continuous_futures"
        else:
            asset_part = asset_class.value
        # Get frequency part.
        freq_part = self.FREQ_PATH_MAPPING[frequency]
        # Get extension.
        if ext == icdtyp.Extension.CSV:
            extension_part = "csv.gz"
        else:
            raise ValueError("Unsupported extension %s" % ext)
        # Get file name.
        file_name = "%s.%s" % (symbol, extension_part)
        # Construct full path.
        file_path = os.path.join(
            iidcon.S3_PREFIX, asset_part, freq_part, file_name
        )
        return file_path
