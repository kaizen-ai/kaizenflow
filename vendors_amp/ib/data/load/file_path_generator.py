import os
from typing import Optional

import vendors_amp.common.data.load.file_path_generator as vcdlfi
import vendors_amp.common.data.types as vcdtyp
import vendors_amp.ib.data.config as vidcon


class IbFilePathGenerator(vcdlfi.FilePathGenerator):
    """
    Generate file path for a symbol.

    File path is: <symbol>_<frequency>.<extension>
    """

    FREQ_PATH_MAPPING = {
        vcdtyp.Frequency.Daily: "day",
        vcdtyp.Frequency.Minutely: "minute",
        vcdtyp.Frequency.Tick: "tick",
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
        Get the path to a specific IB dataset on s3.

        Parameters as in `read_data`.
        :return: path to the file
        """
        freq_part = self.FREQ_PATH_MAPPING[frequency]
        # Format <symbol>_<frequency>.<extension>.
        relative_file_path = "%s_%s.%s" % (symbol, freq_part, ext.value)
        file_path = os.path.join(vidcon.S3_PREFIX, relative_file_path)
        return file_path
