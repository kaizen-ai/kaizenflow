"""
# Import as:

import helpers.s3 as hs3
"""


def get_s3_bucket_path():
    """
    Return the path corresponding to the default s3 bucket.
    Make sure your ~/.aws/credentials uses the right key to access this bucket
    as default.
    """
    # s3_path = "s3://alphamatic"
    s3_path = "s3://default00-bucket"
    return s3_path
