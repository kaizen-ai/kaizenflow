import os
from typing import Optional

import instrument_master.common.data.load.file_path_generator as vcdlfi
import instrument_master.common.data.types as vcdtyp
import instrument_master.ib.data.config as vidcon


class IbFilePathGenerator(vcdlfi.FilePathGenerator):
    """
    Generate file path for a symbol.

    File path is: <symbol>_<frequency>.<extension>
    """

    FREQ_PATH_MAPPING = {
        vcdtyp.Frequency.Daily: "daily",
        vcdtyp.Frequency.Hourly: "hourly",
        vcdtyp.Frequency.Minutely: "minutely",
        vcdtyp.Frequency.Tick: "tick",
    }

    def generate_file_path(
        self,
        symbol: str,
        frequency: vcdtyp.Frequency,
        asset_class: vcdtyp.AssetClass = vcdtyp.AssetClass.Futures,
        contract_type: Optional[vcdtyp.ContractType] = None,
        unadjusted: Optional[bool] = None,
        ext: vcdtyp.Extension = vcdtyp.Extension.CSV,
    ) -> str:
        """
        Get the path to a specific IB dataset on S3.

        Format like `continuous_futures/daily/ES.csv.gz`.

        Parameters as in `read_data`.
        :return: path to the file
        """
        # Get asset class part.
        if (
            contract_type == vcdtyp.ContractType.Continuous
            and asset_class == vcdtyp.AssetClass.Futures
        ):
            asset_part = "continuous_futures"
        else:
            asset_part = asset_class.value
        # Get frequency part.
        freq_part = self.FREQ_PATH_MAPPING[frequency]
        # Get extension.
        if ext == vcdtyp.Extension.CSV:
            extension_part = "csv.gz"
        else:
            raise ValueError("Unsupported extension %s" % ext)
        # Get file name.
        file_name = "%s.%s" % (symbol, extension_part)
        # Construct full path.
        file_path = os.path.join(
            vidcon.S3_PREFIX, asset_part, freq_part, file_name
        )
        return file_path
