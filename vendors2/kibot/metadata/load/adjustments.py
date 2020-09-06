import csv
import os
from typing import List

import smart_open

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

        # I use smart_open.open to allow file_path to both be a local file, or an s3 file,
        # or any other source that `smart_open` supports, which is a lot.
        with smart_open.open(file_path, "r") as fh:
            reader = csv.reader(fh, delimiter="\t")
            # Skip header.
            next(reader)
            ret = [types.Adjustment(*row) for row in reader]
        return ret
