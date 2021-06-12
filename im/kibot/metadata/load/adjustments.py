import os
from typing import List

import pandas as pd

import helpers.pandas_helpers as pdhelp
import im.kibot.metadata.config as vkmcon
import im.kibot.metadata.types as vkmtyp


class AdjustmentsLoader:
    @staticmethod
    def load(symbol: str) -> List[vkmtyp.Adjustment]:
        s3_path = os.path.join(
            vkmcon.S3_PREFIX, vkmcon.ADJUSTMENTS_SUB_DIR, f"{symbol}.txt"
        )
        aws_profile = "am"
        sep = "\t"
        df = pdhelp.read_csv(s3_path, sep=sep)
        return [vkmtyp.Adjustment(*row) for row in df.values.tolist()]
