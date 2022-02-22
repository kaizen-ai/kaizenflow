"""
Import as:

import im_v2.kibot.metadata.load.adjustments as imvkmload
"""

import os
from typing import List

import core.pandas_helpers as cpanh
import helpers.hs3 as hs3
import im_v2.kibot.metadata.config as imvkimeco
import im_v2.kibot.metadata.types as imvkimety


class AdjustmentsLoader:

    @staticmethod
    def load(symbol: str) -> List[imvkimety.Adjustment]:
        s3_path = os.path.join(
            imvkimeco.S3_PREFIX, imvkimeco.ADJUSTMENTS_SUB_DIR, f"{symbol}.txt"
        )
        sep = "\t"
        s3fs = hs3.get_s3fs("am")
        df = cpanh.read_csv(s3_path, s3fs=s3fs, sep=sep)
        return [imvkimety.Adjustment(*row) for row in df.values.tolist()]