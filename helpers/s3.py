"""
# Import as:

import helpers.s3 as hs3
"""
import os
from typing import List

import boto3


def get_path():
    """
    Return the path corresponding to the default s3 bucket.
    Make sure your ~/.aws/credentials uses the right key to access this bucket
    as default.
    """
    # s3_path = "s3://alphamatic"
    s3_path = "s3://default00-bucket"
    return s3_path


def listdir(s3_path: str) -> List[str]:
    """
    List files in s3 directory.

    :param s3_path: The path to s3 directory
    :return:
    """
    AMAZON_MAX_INT = 2147483647
    split_path = s3_path.split("/")
    s3_bucket = split_path[2]
    dir_path = "/".join(split_path[3:])
    s3 = boto3.client("s3")
    s3_objects = s3.list_objects_v2(
        Bucket=s3_bucket, StartAfter=dir_path, MaxKeys=AMAZON_MAX_INT
    )
    contents = s3_objects["Contents"]
    file_names = [cont["Key"] for cont in contents]
    file_names = [
        os.path.basename(file_name)
        for file_name in file_names
        if os.path.dirname(file_name) == dir_path
    ]
    return file_names
