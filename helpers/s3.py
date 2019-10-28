"""
# Import as:

import helpers.s3 as hs3
"""
from typing import List

import boto3

import helpers.dbg as dbg


def get_path():
    """
    Return the path corresponding to the default s3 bucket.
    Make sure your ~/.aws/credentials uses the right key to access this bucket
    as default.
    """
    # s3_path = "s3://alphamatic"
    s3_path = "s3://default00-bucket"
    return s3_path


def is_s3_path(path):
    return path.startswith("s3://")


def listdir(s3_path: str, mode: str = "recursive") -> List[str]:
    """
    List files in s3 directory.

    :param s3_path: the path to s3 directory, e.g.,
        `s3://default00-bucket/kibot/`
    :param mode: recursive or non-recursive
        If recursive, will list all files in the directory
        recursively. If non-recursive, will list only the top-level
        contents of the directory.
    :return: list of paths
    """
    # Parse the s3_path, extracting bucket and directory.
    AMAZON_MAX_INT = 2147483647
    dbg.dassert(is_s3_path(s3_path), "s3 path should start with s3://")
    if not s3_path.endswith("/"):
        s3_path = s3_path + "/"
    split_path = s3_path.split("/")
    # For a s3://default00-bucket/kibot/All_Futures_Continuous_Contracts_daily
    # s3_path the split_path would be ['s3:', '', 'default00-bucket',
    # 'kibot', 'All_Futures_Continuous_Contracts_daily', ''].
    s3_bucket = split_path[2]
    dir_path = "/".join(split_path[3:])
    # Create an s3 object to query.
    s3 = boto3.client("s3")
    s3_objects = s3.list_objects_v2(
        Bucket=s3_bucket, StartAfter=dir_path, MaxKeys=AMAZON_MAX_INT
    )
    contents = s3_objects["Contents"]
    # Extract the `Key` from each element, which contains the name of
    # the file.
    file_names = [cont["Key"] for cont in contents]
    # Remove the initial `dir_path` from the files.
    file_names = [
        file_name.replace(dir_path, "", 1)
        for file_name in file_names
        if file_name.startswith(dir_path)
    ]
    # In s3 file system, there is no such thing as directories (the
    # dir_path is just a part of a file name). So to extract top-level
    # components of a directory, we need to extract the first parts of
    # the file names inside this directory.
    #
    # Let's say we are trying to list the contents of `upper_dir`,
    # which contains `lower_dir1` with a `lower_dir1/file_name1` file.
    # At this point, file_names would be `[lower_dir1/file_name1]`
    # If the `mode` is `non-recursive`, we extract `lower-dir` from
    # each file name in this list. If the `mode` is `recursive`, we
    # leave the file names untouched.
    if mode == "recursive":
        paths = file_names
    elif mode == "non-recursive":
        paths = [file_name.split("/")[0] for file_name in file_names]
        paths = list(set(paths))
    else:
        raise ValueError(
            "Only recursive and non-recursive modes are supported, passed %s."
            % mode
        )
    paths.sort()
    return paths
