"""
Import as:

import helpers.hsftp as hsftp
"""
import logging
import os
import subprocess
from io import BytesIO
from typing import List

subprocess.call(["sudo", "/venv/bin/pip", "install", "pysftp"])

import pysftp

import helpers.haws as haws
import helpers.hsecrets as hsecret

# Create a logger instance.
_LOG = logging.getLogger(__name__)


def get_sftp_connection(hostname: str, secret_name: str) -> pysftp.Connection:
    """
    Return SFTP connection object using a private key stored in AWS Secrets
    Manager.

    :param hostname: hostname of the SFTP server.
    :param secret_name: name of the secret in AWS Secrets Manager
        containing the private key.
    :return: active SFTP connection object.
    """
    # Fetch the private key from AWS Secrets Manager
    secret_dict = hsecret.get_secret(secret_name)
    username = secret_dict["username"]
    private_key = secret_dict["private_key"]

    # Write the private key to a temporary file
    with open("/tmp/temp_key.pem", "w") as temp_key_file:
        temp_key_file.write(private_key)
    # Ensure the key file has the correct permissions
    os.chmod("/tmp/temp_key.pem", 0o600)
    # Ensure pysftp is installed before attempting connection.
    cnopts = pysftp.CnOpts()
    # Disable host key checking.
    cnopts.hostkeys = None
    sftp = pysftp.Connection(
        hostname,
        username=username,
        private_key="/tmp/temp_key.pem",
        cnopts=cnopts,
    )
    # Remove the temporary key file after establishing the connection
    os.remove("/tmp/temp_key.pem")
    return sftp


def download_file_to_s3(
    sftp: pysftp.Connection,
    s3_client: haws.BaseClient,
    remote_dir: str,
    filename: str,
    s3_bucket: str,
    s3_prefix: str,
) -> None:
    """
    Download data from an SFTP server and upload it to an S3 bucket.

    :param sftp: An active SFTP Connection object.
    :param s3_client: An AWS Base client object to interact with S3.
    :param remote_dir: The directory on the SFTP server where the file
        is located.
    :param filename: The name of the file to download from the SFTP
        server.
    :param s3_bucket: The name of the S3 bucket to upload the file to.
    :param s3_prefix: The prefix (path) in the S3 bucket where the file
        will be stored.
    :return: None.
    """
    remote_path = f"{remote_dir}/{filename}"
    s3_key = f"{s3_prefix}/{filename}"
    with sftp.open(remote_path) as file_obj:
        # Download data from sftp server.
        file_data = file_obj.read()
        try:
            # Upload data to S3.
            s3_client.upload_fileobj(BytesIO(file_data), s3_bucket, s3_key)
            _LOG.info(
                "Uploaded: %s to s3://%s/%s", remote_path, s3_bucket, s3_key
            )
        except Exception as e:
            _LOG.error("Failed to upload file to S3. Error: %s", str(e))
            raise e


def get_file_names(sftp: pysftp.Connection, sftp_remote_dir: str) -> List[str]:
    """
    Retrieve all file names from a specified directory on a remote SFTP server.

    :param sftp: An active SFTP Connection object.
    :param sftp_remote_dir: The directory on the SFTP server from which
        to list file names.
    :return: A list of file names present in the specified directory on
        the SFTP server.
    """
    file_names = []
    for item in sftp.listdir_attr(sftp_remote_dir):
        file_names.append(item.filename)
    return file_names
