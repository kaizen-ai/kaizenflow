"""
# Import as:

import helpers.s3 as hs3
"""
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


def listdir(s3_path: str, mode: str = "recursive") -> List[str]:
    """
    List files in s3 directory.

    :param s3_path: the path to s3 directory
    :param mode: recursive or non-recursive
        If recursive, will list all files in the directory
        recursively. If non-recursive, will list only the next level of
        the directory.
    :return: list of file names
    """
    AMAZON_MAX_INT = 2147483647
    if not s3_path.endswith("/"):
        s3_path = s3_path + "/"
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
        file_name.replace(dir_path, "", 1)
        for file_name in file_names
        if file_name.startswith(dir_path)
    ]
    if mode == "recursive":
        file_names = [file_name.split("/")[0] for file_name in file_names]
        file_names = list(set(file_names))
    elif mode == "non-recursive":
        pass
    else:
        raise ValueError(
            "Only recursive and non-recursive modes are supported, passed %s."
            % mode
        )
    return file_names
