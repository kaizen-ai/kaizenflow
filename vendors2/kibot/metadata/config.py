import os

import helpers.s3 as hs3


class Config:
    S3_PATH = os.path.join(hs3.get_path(), "kibot/metadata")
