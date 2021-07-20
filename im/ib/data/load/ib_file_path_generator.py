import functools
import logging
import os
from typing import Optional

import helpers.dbg as dbg
import helpers.s3 as hs3
import im.common.data.load.file_path_generator as icdlfi
import im.common.data.types as icdtyp
import im.ib.data.config as iidcon

_LOG = logging.getLogger(__name__)


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
        asset_class: icdtyp.AssetClass,
        contract_type: Optional[icdtyp.ContractType] = None,
        exchange: Optional[str] = None,
        currency: Optional[str] = None,
        unadjusted: Optional[bool] = None,
        ext: icdtyp.Extension = icdtyp.Extension.CSV,
    ) -> str:
        """
        Get the path to a specific IB dataset on S3.

        Format like `continuous_futures/daily/ES.csv.gz`.

        Parameters as in `read_data`.
        :return: path to the file
        :raises ValueError: if parameters are incompatible
        """
        # Check parameters.
        dbg.dassert_is_not(currency, None)
        dbg.dassert_is_not(exchange, None)
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
            iidcon.S3_PREFIX, asset_part, exchange, currency, freq_part, file_name
        )
        return file_path

    @staticmethod
    @functools.lru_cache(maxsize=16)
    def get_latest_symbols_file() -> str:
        """
        Get the latest available file with symbols on S3.
        """
        file_prefix = os.path.join(iidcon.S3_METADATA_PREFIX, "symbols-")
        s3fs = hs3.get_s3fs("am")
        files = s3fs.glob(file_prefix + "*")
        _LOG.debug("files='%s'", files)
        latest_file: str = max(files)
        _LOG.debug("latest_file='%s'", latest_file)
        # Add the prefix.
        latest_file = os.path.join(iidcon.S3_METADATA_PREFIX, latest_file)
        dbg.dassert(s3fs.exists(latest_file))
        return latest_file
