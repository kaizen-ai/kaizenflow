"""
# Import as:

import helpers.s3 as hs3
"""

import logging
import os
from typing import List, Tuple

import boto3

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


def get_bucket() -> str:
    """
    Return the default s3 bucket.
    Make sure your ~/.aws/credentials uses the right key to access this bucket
    as default.
    """
    # s3_bucket = "alphamatic"
    s3_bucket = "default00-bucket"
    return s3_bucket


# TODO(gp): -> get_root_path() ?
def get_path() -> str:
    """
    Return the path corresponding to the default s3 bucket.
    """
    s3_path = "s3://" + get_bucket()
    return s3_path


def parse_path(path: str) -> Tuple[str, str]:
    """
    Extract bucket name and a file or folder path from an s3 full path.
    E.g., for
        s3://default00-bucket/kibot/All_Futures_Continuous_Contracts_daily
    the result is:
        bucket_name = "default00-bucket"
        path = "kibot/All_Futures_Continuous_Contracts_daily"
    """
    prefix = "s3://"
    dbg.dassert(
        path.startswith(prefix),
        "Path='%s' doesn't start with prefix '%s",
        path,
        prefix,
    )
    path = path[len(prefix):]
    path = path.split("/")
    dbg.dassert(path, "The %s path is empty.", path)
    bucket_name = path[0]
    bucket_name = bucket_name.replace("s3://", "")
    path = "/".join(path[1:])
    return bucket_name, path


def ls(file_path: str) -> List[str]:
    """
    Return the file lists in `file_path`
    """
    s3 = boto3.resource("s3")
    bucket_name, file_path = parse_path(file_path)
    _LOG.debug("bucket_name=%s, file_path=%s", bucket_name, file_path)
    my_bucket = s3.Bucket(bucket_name)
    res = my_bucket.objects.filter(Prefix=file_path)
    # list(res.all())[0] looks like:
    #   s3.ObjectSummary(bucket_name='default00-bucket',
    #       key='kibot/All_Futures_Continuous_Contracts_daily/AC.csv.gz')
    file_names = [os.path.basename(p.key) for p in res.all()]
    return file_names
