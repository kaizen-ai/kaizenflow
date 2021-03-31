import os
from typing import List

import pandas as pd

import instrument_master.kibot.metadata.config as vkmcon
import instrument_master.kibot.metadata.types as vkmtyp


class AdjustmentsLoader:
    @staticmethod
    def load(symbol: str) -> List[vkmtyp.Adjustment]:
        s3_path = os.path.join(
            vkmcon.S3_PREFIX, vkmcon.ADJUSTMENTS_SUB_DIR, f"{symbol}.txt"
        )

        df = pd.read_csv(s3_path, sep="\t")
        return [vkmtyp.Adjustment(*row) for row in df.values.tolist()]
