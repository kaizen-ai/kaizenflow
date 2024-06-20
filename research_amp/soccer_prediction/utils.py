"""
Import as:

import research_amp.soccer_prediction.utils as rasoprut
"""

import logging
import os
from typing import Any, Dict

import pandas as pd

import helpers.haws as haws
import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def download_data_from_s3(
    bucket_name: str,
    dataset_path: str,
    local_path: str,
    file_format: str = ".txt",
    logging_level: int = logging.INFO,
) -> None:
    """
    Function to download files from S3.

    :param bucket_name: S3 bucket name.
    :param dataset_path: Path for the dataset in the S3 bucket.
    :param local_path: Destination path for downloading the dataset from
        the S3 to local machine.
    """
    # Initialize S3 session.
    s3 = haws.get_service_resource(aws_profile="ck", service_name="s3")
    # Fetch S3 bucket.
    bucket = s3.Bucket(bucket_name)
    # Define the local directory to save the files.
    os.makedirs(local_path, exist_ok=True)
    # Download the files the S3 path recursively.
    bucket = s3.Bucket(bucket_name)
    objects = list(bucket.objects.filter(Prefix=dataset_path))
    # Check for Null objects.
    if not objects:
        msg = "No files present in the S3 Location: "
        s3_path = f"s3://{bucket}/{dataset_path}"
        hdbg.dassert_eq(0, len(objects), msg, s3_path)
    for obj in bucket.objects.filter(Prefix=dataset_path):
        key = obj.key
        # Select the files that end with specified format.
        if key.endswith(file_format):
            local_file_path = os.path.join(local_path, os.path.basename(key))
            _LOG.log(logging_level, f"Downloading {key} to {local_file_path}")
            bucket.download_file(key, local_file_path)
    _LOG.log(logging_level, "Data Downloaded.")


def load_data_to_dataframe(
    local_path: str,
    file_format: str = ".txt",
    logging_level: int = logging.INFO,
    sep: str = "\t",
    encoding: str = "UTF-8",
    **kwargs: Any,
) -> Dict:
    """
    Function to load datasets into pandas dataframe.

    :param local_path: Local directory where the S3 data was downloaded.
    :param file_format: The format of the files to be loaded. Default is
        ".txt".
    :param logging_level: Logging level. Default is logging.INFO.
    :param kwargs: Additional arguments to pass to pandas read_csv.
    :return: Dictionary of the datasets downloaded.
    """
    dataframes = {}
    # Iterate in the directory to collect the files and load them into dataframes.
    for dirname, _, filenames in os.walk(local_path):
        for filename in filenames:
            if filename.endswith(file_format):
                file_key = filename.split(".")[0] + "_df"
                filepath = os.path.join(dirname, filename)
                _LOG.log(logging_level, f"Loading {filepath}")
                df = pd.read_csv(filepath, sep=sep, encoding=encoding, **kwargs)
                _LOG.log(
                    logging_level,
                    f" {file_key},  {df.shape}",
                )
                # Check if the dataframe is empty.
                if df.empty:
                    hdbg.dassert_eq(0, df.shape[0], "Empty Dataframe: ", file_key)
                # Drop duplicates.
                df = df.drop_duplicates()
                # Append to dictionary.
                dataframes[file_key] = df
    _LOG.log(logging_level, "Data loaded into dataframes.")
    # Return the dictionary of the dataframes.
    return dataframes

def save_data_to_s3(
        df: pd.DataFrame(),
        bucket_name: str,
        s3_path: str,
        file_name: str,
        index: bool = False,
        **kwargs: Any,
        ) -> None:
    """
    Saves the give dataframe in S3  location as a `.csv` file.

    :param:
    """
    # Check if given dataframe is empty.
    if df.empty:
        raise ValueError("The given dataframe is empty.")
    # Save the dataframe a csv file in local directory. 
    file_path = file_name + ".csv"
    df.to_csv(file_path, index=index)
    # Initiate S3 session.
    s3 = haws.get_service_resource(aws_profile="ck", service_name="s3")
    # Upload the file to S3.
    save_path = s3_path + "/" + file_path
    s3.Bucket(bucket_name).upload_file(file_path, save_path)