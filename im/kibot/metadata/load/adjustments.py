import os
from typing import List

import core.pandas_helpers as pdhelp
import helpers.s3 as hs3
import im.kibot.metadata.config as vkmcon
import im.kibot.metadata.types as vkmtyp


class AdjustmentsLoader:
    @staticmethod
    def load(symbol: str) -> List[vkmtyp.Adjustment]:
        s3_path = os.path.join(
            vkmcon.S3_PREFIX, vkmcon.ADJUSTMENTS_SUB_DIR, f"{symbol}.txt"
        )
        sep = "\t"
        s3fs = hs3.get_s3fs("am")
        df = pdhelp.read_csv(s3_path, s3fs=s3fs, sep=sep)
        return [vkmtyp.Adjustment(*row) for row in df.values.tolist()]
