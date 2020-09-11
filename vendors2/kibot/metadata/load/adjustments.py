import os
from typing import List

import pandas as pd

import vendors2.kibot.metadata.config as config
import vendors2.kibot.metadata.types as types


class AdjustmentsLoader:
    @staticmethod
    def load(symbol: str) -> List[types.Adjustment]:
        s3_path = os.path.join(
            config.S3_PREFIX, config.ADJUSTMENTS_SUB_DIR, f"{symbol}.txt"
        )

        df = pd.read_csv(s3_path, sep="\t")
        return [types.Adjustment(*row) for row in df.values.tolist()]
