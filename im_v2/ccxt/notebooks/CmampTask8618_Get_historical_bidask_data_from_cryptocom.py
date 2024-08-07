# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# !sudo /venv/bin/pip install pysftp

# %%
# Importing modules.
import gzip
import logging
import os
from io import BytesIO

import pandas as pd
import pysftp

import helpers.haws as haws
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import helpers.hs3 as hs3

# %%
hdbg.init_logger(verbosity=logging.INFO)
log_level = logging.INFO

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# Configuration
config = {
    "stage": "test",
    "save_path_prefix": "sonaal/cryptocom/historical_bid_ask/",
    "hostname": "data.crypto.com",
    "username": "user005",
    "private_key_path": "/app/amp/cryptocom-privatekey.pem",
    # Download config.
    "currency_pair": "BTC_USDT",
    "date": "2023-10-01",
}

# Disable host key checking
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None


# %%
def sftp_to_s3(sftp, remote_dir, s3_bucket, s3_prefix):
    """
    Download data from sftp server and upload to S3 bucket.
    """
    for item in sftp.listdir_attr(remote_dir):
        remote_path = f"{remote_dir}/{item.filename}"
        s3_key = f"{s3_prefix}/{item.filename}" if s3_prefix else item.filename
        with sftp.open(remote_path) as file_obj:
            file_data = file_obj.read()
            s3_client.upload_fileobj(BytesIO(file_data), s3_bucket, s3_key)
            _LOG.info(f"Uploaded: {remote_path} to s3://{s3_bucket}/{s3_key}")


# %%
def load_data(
    s3: boto3.client,
    s3_path: str,
    s3_bucket: str,
) -> list:
    """
    Loads and performs a QA check on files in a specified S3 bucket and path.

    This function lists all objects in a given S3 path, downloads each
    file, checks for empty files, reads and parses the content of
    gzipped JSON files, and collects the data into a list of pandas
    DataFrames.

    :param s3: an S3 client object from boto3.
    :param s3_path: S3 path (prefix) to list and load files from.
    :param s3_bucket: name of the S3 bucket.
    :return: list of pandas DataFrames containing the data from the
        files.
    """
    dataframes = []
    # List all objects in the specified S3 bucket and path
    files = haws.list_all_objects(s3, s3_bucket, s3_path)
    for file in files:
        # Check for empty files
        if file["Size"] == 0:
            _LOG.info("Found empty file %s", file["Key"])
            continue
        # Download the file from S3 to a local temporary path
        local_dst_path = "tmp.data.gz"
        s3_file_path = file["Key"]
        s3.download_file(s3_bucket, s3_file_path, local_dst_path)
        # Read and parse the gzipped JSON file
        with gzip.open(local_dst_path, "rt") as gz_file:
            file_content = gz_file.read()
            df = pd.read_json(file_content, lines=True)
        # Append the DataFrame to the list
        dataframes.append(df)
    return dataframes


# %%
hostname = config["hostname"]
username = config["username"]
private_key = config["private_key_path"]

s3_client = haws.get_service_client(aws_profile="ck", service_name="s3")
bucket_name = hs3.get_s3_bucket_from_stage(stage=config["stage"])
prefix = config["save_path_prefix"]

currency_pair = config["currency_pair"]
date = pd.to_datetime(config["date"])
year = date.year
month = date.month
day = date.day

sftp_data_path = (
    f"/exchange/book_l2_150_0010/{year}/{month}/{day}/cdc/{currency_pair}"
)

s3_save_path = os.path.join(prefix, currency_pair, config["date"])

# Establish the SFTP connection
with pysftp.Connection(
    hostname, username=username, private_key=private_key_path, cnopts=cnopts
) as sftp:
    print("Connection successfully established ...")
    # Start the recursive download from the remote root directory
    sftp_to_s3(sftp, sftp_data_path, bucket_name, s3_save_path)

print("All files have been downloaded successfully.")

data = load_data(s3, s3_save_path, bucket_name)

# %%
data = load_data(s3_client, s3_save_path, bucket_name)

# %%
data[0].head()

# %%
