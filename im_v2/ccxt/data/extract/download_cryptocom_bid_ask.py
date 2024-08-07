#!/usr/bin/env python
"""
This Python script shows how to download the Historical Future Order Book level
2 Data via API, also perform QA on the downloaded data.

QA includes:
 - Check if non empyty is downloaded.
 - Check correct file is downloaded for correct symbol.

> im_v2/ccxt/data/extract/download_cryptocom_bid_ask.py \
    --start_date '2024-06-01' \
    --stop_date '2024-06-03' \
    --universe 'v8.1' \
    --stage 'test' \
    --save_path_prefix 'sonaal/cryptocom/historical_bid_ask' \
    --incremental
"""

import argparse
import logging
import os
import tempfile
from typing import List

import pandas as pd
import tqdm
from botocore.exceptions import ClientError

import helpers.haws as haws
import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hs3 as hs3
import helpers.hsftp as hsftp
import im_v2.common.universe.universe as imvcounun

_LOG = logging.getLogger(__name__)

CONFIG = {
    "hostname": "data.crypto.com",
    "secret_name": "cryptocom.downloading.1",
}


def upload_directory_to_s3(
    s3_client: haws.BaseClient, local_dir: str, s3_bucket: str, s3_prefix: str
) -> None:
    """
    Upload all files from a local directory to an S3 bucket.

    :param s3_client: S3 client object.
    :param local_dir: Local directory containing the files to upload.
    :param s3_bucket: Name of the destination S3 bucket.
    :param s3_prefix: Prefix (path) in the S3 bucket where the files
        will be stored.
    :return: return None.
    """
    for root, _, files in tqdm.tqdm(os.walk(local_dir)):
        for file in tqdm.tqdm(files):
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_dir)
            s3_path = os.path.join(s3_prefix, relative_path)
            try:
                s3_client.upload_file(local_path, s3_bucket, s3_path)
                _LOG.info(
                    "Uploaded: %s to s3://%s/%s", local_path, s3_bucket, s3_path
                )
            except Exception as e:
                raise e


def check_directory_exists_on_s3(
    s3: haws.BaseClient, s3_bucket: str, s3_prefix: str
) -> bool:
    """
    Check if a directory exists at the specified S3 path.

    :param s3: AWS client to access S3 bucket.
    :param s3_bucket: S3 bucket name.
    :param s3_prefix: S3 directory path.
    :return: return True if the directory exists, False otherwise.
    """
    try:
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
        if "Contents" in response:
            return True
    except ClientError as e:
        # If the error is not a 404, re-raise the exception
        _LOG.warning(
            "Error occurred while checking for directory %s in bucket %s : %s",
            s3_prefix,
            s3_bucket,
            e,
        )
        raise e
    return False


def download_data_to_s3(
    s3_client: haws.BaseClient,
    sftp: hsftp.pysftp.Connection,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    currency_pair: str,
    s3_bucket: str,
    incremental: bool,
    save_path_prefix: str,
) -> None:
    """
    Download historical data from crypto.com and upload it to an S3 bucket.

    :param s3_client: AWS Baseclient object.
    :param sftp: pysftp Connection object.
    :param start_date: start date for the data download.
    :param end_date: end date for the data download.
    :param currency_pair: currency pair for which data is to be
        downloaded.
    :param s3_bucket: S3 bucket name where the data will be uploaded.
    :param incremental:
        - if `True` and downloaded is valid (QA passed) then skip download.
        - if `False` download data even if it was already downloaded.
    :param save_path_prefix: prefix path to store data in bucket, e.g. s3://<s3_bucket>/<save_path_prefix>.

    :return: return None.
    """
    # Generate a list of dates between start_date and end_date.
    timestamp_list = pd.date_range(start=start_date, end=end_date, freq="D")
    date_list = [timestamp.date() for timestamp in timestamp_list]
    for date in tqdm.tqdm(date_list):
        year = date.year
        month = date.month
        day = date.day
        # set a sftp server data path with necessary parameters.
        sftp_data_path = (
            f"/exchange/book_l2_150_0010/{year}/{month}/{day}/cdc/{currency_pair}"
        )
        s3_save_path_prefix = os.path.join(
            save_path_prefix, currency_pair, str(date)
        )
        # download a directory if it does not already exist on S3.
        if incremental and check_directory_exists_on_s3(
            s3_client, s3_bucket, s3_save_path_prefix
        ):
            _LOG.info(
                "Data already exists for currency_pair: %s and for date: %s. Skipping ...",
                currency_pair,
                date,
            )
        else:
            with tempfile.TemporaryDirectory() as tmpdirname:
                sftp.get_r(sftp_data_path, tmpdirname, preserve_mtime=True)
                upload_directory_to_s3(
                    s3_client,
                    tmpdirname,
                    s3_bucket,
                    s3_save_path_prefix,
                )


def download_data_to_s3_with_exception(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    currency_pairs: List[str],
    s3_bucket: str,
    incremental: bool,
    save_path_prefix: str,
) -> List[str]:
    """
    Check `download_data_to_s3()` for param description.

    :return: return list of currency_pairs failed.
    """
    currency_pairs_failed = []
    # Initialize the S3 client.
    s3_client = haws.get_service_client(aws_profile="ck", service_name="s3")
    # Initialize parameters.
    hostname = CONFIG["hostname"]
    secret_name = CONFIG["secret_name"]
    # Get SFTP Connection object.
    sftp = hsftp.get_sftp_connection(hostname, secret_name)
    _LOG.info("Downloading for... %s - %s", start_date, end_date)
    for currency_pair in currency_pairs:
        _LOG.info("--------------- %s ------------------", currency_pair)
        try:
            download_data_to_s3(
                s3_client,
                sftp,
                start_date,
                end_date,
                currency_pair,
                s3_bucket=s3_bucket,
                incremental=incremental,
                save_path_prefix=save_path_prefix,
            )
        except Exception as e:
            _LOG.info(
                "Failed to download for %s from %s to %s",
                currency_pair,
                start_date,
                end_date,
            )
            _LOG.exception(e)
            currency_pairs_failed.append(currency_pair)
    sftp.close()
    return currency_pairs_failed


def _parse() -> argparse.ArgumentParser:
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Script to download historical data from Binance and upload to S3.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser = hparser.add_verbosity_arg(parser)
    parser.add_argument(
        "--universe",
        required=False,
        default="v8.1",
        type=str,
        help="Universe version",
    )
    parser.add_argument(
        "--start_date",
        required=True,
        type=str,
        help="Start date for the data download (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--stop_date",
        required=True,
        type=str,
        help="End date for the data download (YYYY-MM-DD), this date will not get downloaded",
    )
    parser.add_argument(
        "--stage",
        action="store",
        type=str,
        default="test",
        help="Stage to run at: test, preprod",
    )
    parser.add_argument(
        "--incremental",
        required=False,
        action="store_true",
        help="If True skips downloading if QA passes",
    )
    parser.add_argument(
        "--save_path_prefix",
        required=False,
        default="sonaal/cryptocom/historical_bid_ask",
        type=str,
        help="Prefix path to dump data, s3://<bucket>/<save_path_prefix>",
    )
    return parser


def _run(args):
    """
    Main execution function that handles the download process.

    :param args: Command line arguments parsed by argparse.
    """
    # Get currency pairs to download from universe.
    vendor_name = "ccxt"
    mode = "download"
    version = args.universe
    universe = imvcounun.get_vendor_universe(vendor_name, mode, version=version)
    # currency_pairs = universe["cryptocom"]
    # TODO(Sonaal): Currently we are only interested BTC/ETH for spot and futures
    # due to low volatility in other symbols.
    currency_pairs = ["ETHUSD-PERP", "BTCUSD-PERP", "BTC_USDT", "ETH_USDT"]
    # Create S3 path to dump data.
    s3_bucket = hs3.get_s3_bucket_from_stage(stage=args.stage)
    start_date = pd.Timestamp(args.start_date)
    end_date = pd.Timestamp(args.stop_date) - pd.Timedelta(days=1)
    # Download data for the entire date range.
    currency_pairs_failed = download_data_to_s3_with_exception(
        start_date,
        end_date,
        currency_pairs,
        s3_bucket=s3_bucket,
        incremental=args.incremental,
        save_path_prefix=args.save_path_prefix,
    )
    if currency_pairs_failed:
        msg = f"The following currency pairs failed: {currency_pairs_failed}"
        hdbg.dfatal(msg)


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _run(args)


if __name__ == "__main__":
    _main(_parse())
