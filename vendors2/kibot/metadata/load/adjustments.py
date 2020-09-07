import os
from typing import List

import pandas as pd

import vendors2.kibot.metadata.config as config
import vendors2.kibot.metadata.types as types


class AdjustmentsLoader:
    @staticmethod
    def load() -> List[types.Adjustment]:
        file_path = os.path.join(
            config.S3_PREFIX,
            config.ADJUSTMENTS_SUB_DIR,
            config.ADJUSTMENTS_FILE_NAME,
        )

        df = pd.read_csv(file_path, sep="\t")
        return [types.Adjustment(*row) for row in df.values.tolist()]


if __name__ == "__main__":
    print(AdjustmentsLoader().load())
