"""
Import as:

import im.ib.data.config as imibdacon
"""

import os

import helpers.s3 as hs3

S3_BUCKET = hs3.get_bucket()
S3_PREFIX = f"s3://{S3_BUCKET}/data/ib"
S3_METADATA_PREFIX = os.path.join(S3_PREFIX, "metadata")
