"""
Import as:

import im.ib.data.config as imibdacon
"""

import os

import helpers.hs3 as hs3

AM_AWS_PROFILE = "am"
S3_BUCKET = hs3.get_s3_bucket_path(AM_AWS_PROFILE, add_s3_prefix=False)
S3_PREFIX = f"s3://{S3_BUCKET}/data/ib"
S3_METADATA_PREFIX = os.path.join(S3_PREFIX, "metadata")
