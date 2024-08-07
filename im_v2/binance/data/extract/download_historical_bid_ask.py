#!/usr/bin/env python
"""
This Python script shows how to download the Historical Future Order Book level
2 Data via API, also perform QA on the downloaded data.

QA includes:
 - Check if non empyty is downloaded.
 - Check correct file is downloaded for correct symbol.

Use as:

> im_v2/binance/data/extract/download_historical_bid_ask.py \
    --start_date '2020-08-01' \
    --stop_date '2020-08-09' \
    --universe 'v8.1' \
    --bid_ask_data_type 'T_DEPTH' \
    --secret_name 'binance.preprod.trading.10' \
    --stage 'test' \
    --save_path_prefix 'sonaal/binance/historical_bid_ask' \
    --incremental

import im_v2.binance.data.extract.download_historical_bid_ask as imbdexdo
"""

import argparse
import logging
import os
import tarfile
import time
from typing import List

import pandas as pd
import requests
from botocore.exceptions import ClientError

import helpers.haws as haws
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hs3 as hs3
import im_v2.binance.data.extract.api_client as imvbdeapcl
import im_v2.common.universe.universe as imvcounun

_LOG = logging.getLogger(__name__)

S_URL_V1 = "https://api.binance.com/sapi/v1/futures"


def fetch_active_symbols() -> List[str]:
    """
    Fetch all symbols currently trading on Binance with status 'active'.
    """
    # Binance API endpoint to get exchange information.
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    response = requests.get(url)
    # Check if the request was successful.
    if response.status_code == 200:
        data = response.json()
        # Retrieve the list of symbols that are actively trading.
        symbols = [
            symbol_info["symbol"]
            for symbol_info in data["symbols"]
            if symbol_info["status"] == "TRADING"
            and symbol_info["contractType"] == "PERPETUAL"
        ]
        return symbols
    else:
        _LOG.warning(
            f"Failed to fetch symbols from {url}. Status code: {response.status_code}"
        )
        return []


def download_and_upload_to_s3(
    s3: haws.BaseClient,
    url: str,
    bucket: str,
    object_key: str,
    output_filename: str,
) -> None:
    """
    Download a file from a URL and upload it to an S3 bucket.

    :param s3: AWS client to access S3 bucket
    :param url: The URL to download the file from.
    :param bucket: The S3 bucket name.
    :param object_key: The S3 object key.
    :param output_filename: The name of the file to save locally.
    """
    # Download the file from the URL
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(output_filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    else:
        _LOG.warning(
            f"Failed to download file from {url}. Status code: {response.status_code}"
        )
        return
    # Upload the file to S3
    try:
        s3.upload_file(output_filename, bucket, object_key)
        _LOG.info("File uploaded to s3://%s/%s", bucket, object_key)
    except Exception as e:
        raise Exception(f"Failed to upload file to S3. Error: {str(e)}")
    # Remove the downloaded file
    os.remove(output_filename)


def check_object_file_exist_on_s3(
    s3: haws.BaseClient, s3_bucket: str, object_key: str
) -> bool:
    """
    Check if a file exists at the specified S3 path.

    :param s3: AWS client to access S3 bucket
    :param s3_bucket: S3 bucket name
    :param object_key: S3 object key
    :return: True if the file exists, False otherwise
    """
    try:
        s3.head_object(Bucket=s3_bucket, Key=object_key)
        return True
    except ClientError as e:
        # Check if the error is a 404 error (object not found)
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            # If the error is not a 404, re-raise the exception
            raise


def check_data_files(
    file_names: List[str], currency_pair: str, data_type: str, qa_date: str
) -> bool:
    """
    Check numeber of files, file names and the timestamp inside the file
    matches.
    """
    # Replace underscore in currency pair to form the expected format
    currency_pair_string = currency_pair.replace("_", "")
    # Define expected file patterns based on data_type
    if data_type == "T_DEPTH":
        expected = [
            f"{currency_pair_string}_{data_type}_{qa_date}_depth_snap.csv",
            f"{currency_pair_string}_{data_type}_{qa_date}_depth_update.csv",
        ]
    elif data_type == "S_DEPTH":
        expected = [f"{currency_pair_string}_{data_type}_{qa_date}.csv"]
    else:
        raise ValueError(f"Invalid data_type: {data_type}")
    # Sort lists to ensure comparison is order-independent
    file_names_sorted = sorted(file_names)
    return file_names_sorted == expected


def check_data_qa(
    s3: haws.BaseClient,
    currency_pair: str,
    data_type: str,
    qa_date: str,
    save_path_prefix: str,
    s3_bucket: str,
) -> bool:
    """
    Perform QA check on the file.
    """
    s3_file_path = (
        f"{save_path_prefix}/{data_type}/{currency_pair}/{qa_date}/data.tar.gz"
    )
    if check_object_file_exist_on_s3(s3, s3_bucket, s3_file_path):
        # Download the .tar.gz file from S3 to a local file
        local_dst_path = "tmp.data.tar.gz"
        s3.download_file(s3_bucket, s3_file_path, local_dst_path)
        if os.path.getsize(local_dst_path) == 0:
            _LOG.info("Data QA failed. Reason: Failed due to empty File")
            return False
        try:
            # Open the tar file to check its contents without extracting
            with tarfile.open(local_dst_path, "r:gz") as tar:
                file_names = tar.getnames()
                return check_data_files(
                    file_names, currency_pair, data_type, qa_date
                )
        except:
            _LOG.info("Data QA failed. Reason: Failed due to partial upload")
            return False
        finally:
            os.remove(local_dst_path)
    else:
        return False


def download_data_to_s3(
    api_client: imvbdeapcl.BinanceAPIClient,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    currency_pair: str,
    data_type: str,
    s3_bucket: str,
    incremental: bool,
    save_path_prefix: str,
) -> bool:
    """
    Download historical data from Binance and upload it to an S3 bucket.

    :param api_client: Instance of the Binance API client
    :param start_date: The start date for the data download
    :param end_date: The end date for the data download
    :param currency_pair: The currency pair for which data is to be
        downloaded
    :param data_type: The type of data to download (e.g., 'T_DEPTH',
        'S_DEPTH').
    :param s3_bucket: The S3 bucket name where the data will be uploaded
    :param incremental:
        - if `True` and downloaded is valid (QA passed) then skip download
        - if `False` download data
    :param save_path_prefix: prefix path to store data in bucket, e.g. s3://<s3_bucket>/<save_path_prefix>

    :return: return True if QA check passed on downloaded data else False
    """
    # Initialize the S3 client.
    s3 = haws.get_service_client(aws_profile="ck", service_name="s3")
    # current timestamp which serves as an input for the params variable.
    timestamp = hdateti.convert_timestamp_to_unix_epoch(
        hdateti.get_current_time("UTC")
    )
    # Generate a list of dates between start_date and end_date, and add each
    # date to the download_dates set if the corresponding file does not already
    # exist in S3.
    timestamp_list = pd.date_range(start=start_date, end=end_date, freq="D")
    date_list = [
        timestamp.date().strftime("%Y-%m-%d") for timestamp in timestamp_list
    ]
    download_dates = set()
    for dates in date_list:
        save_name = (
            f"{save_path_prefix}/{data_type}/{currency_pair}/{dates}/data.tar.gz"
        )
        # If incremental = True and downloaded data is valid (QA passed) then skip.
        if incremental and check_data_qa(
            s3, currency_pair, data_type, dates, save_path_prefix, s3_bucket
        ):
            _LOG.info(
                "Data QA for %s for %s Passed. Skipping ...", currency_pair, dates
            )
            continue
        else:
            download_dates.add(dates)
    # Toggle qa_check False if failed after downloading.
    qa_check = True
    if download_dates:
        start_time = hdateti.convert_timestamp_to_unix_epoch(start_date)
        end_time = hdateti.convert_timestamp_to_unix_epoch(end_date)
        # Calls the "get" function to obtain the download link for the specified
        # symbol, dataType and time range combination.
        params_to_obtain_download_link = {
            "symbol": currency_pair.replace("_", ""),
            "dataType": data_type,
            "startTime": start_time,
            "endTime": end_time,
            "timestamp": timestamp,
        }
        path_to_obtain_download_link = "%s/histDataLink" % S_URL_V1
        result_to_be_downloaded = api_client._get(
            path_to_obtain_download_link, params_to_obtain_download_link
        )
        # Check if the response status code is 429. Hit per minute is exceeded. Terminate.
        if result_to_be_downloaded.status_code == 429:
            hdbg.dfatal("Hit limit exceeded")
        if result_to_be_downloaded.status_code != 200:
            try:
                # Try to parse result_to_be_downloaded JSON if available.
                error_message = result_to_be_downloaded.json()
            except ValueError:
                # If result_to_be_downloaded is not JSON, use
                # result_to_be_downloaded text.
                error_message = result_to_be_downloaded.text
                # Sleep to avoid exceeding hit limit.
                time.sleep(5)
            _LOG.info("%s", error_message)
        _LOG.debug(result_to_be_downloaded)
        urls = result_to_be_downloaded.json()["data"]
        _LOG.debug(urls)
        # Download the URL if the URL's day is in download_dates, and upload the
        # data to S3.
        for url in urls:
            if url["day"] in download_dates:
                download_dates.discard(url["day"])
                save_name = f"{save_path_prefix}/{data_type}/{currency_pair}/{url['day']}/data.tar.gz"
                download_and_upload_to_s3(
                    s3,
                    url["url"],
                    s3_bucket,
                    save_name,
                    f"data_{data_type}.tar.gz",
                )
                _LOG.info(f"Download complete .... {url['day']}")
                # Perform QA check on downloaded data.
                if check_data_qa(
                    s3,
                    currency_pair,
                    data_type,
                    url["day"],
                    save_path_prefix,
                    s3_bucket,
                ):
                    _LOG.info(
                        "Data QA for %s for %s Passed.", currency_pair, url["day"]
                    )
                else:
                    _LOG.warning(
                        "Data QA for %s for %s Failed.", currency_pair, url["day"]
                    )
                    qa_check = False
    return qa_check


def download_data_to_s3_with_exception(
    api_client: imvbdeapcl.BinanceAPIClient,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    currency_pairs: List[str],
    data_type: str,
    s3_bucket: str,
    incremental: bool,
    save_path_prefix: str,
) -> List[str]:
    """
    Check `download_data_to_s3()` for param descption.

    :return: return list of currency_pairs failed.
    """
    currency_pairs_failed = []
    _LOG.info("Downloading for... %s - %s", start_date, end_date)
    for currency_pair in currency_pairs:
        _LOG.info("--------------- %s ------------------", currency_pair)
        try:
            qa_check = download_data_to_s3(
                api_client,
                start_date,
                end_date,
                currency_pair,
                data_type=data_type,
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
            qa_check = False
            _LOG.exception(e)
        finally:
            # QA check failed, store the failed currency pair name for logging.
            if not qa_check:
                currency_pairs_failed.append(currency_pair)
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
        "--bid_ask_data_type",
        required=True,
        type=str,
        help="Type of data to download (e.g., 'T_DEPTH', 'S_DEPTH)",
    )
    parser.add_argument(
        "--secret_name",
        required=True,
        type=str,
        help="Name of the secret for accessing the Binance API",
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
        help="If True skips downloading if path exists",
    )
    parser.add_argument(
        "--save_path_prefix",
        required=False,
        default="binance/historical_bid_ask",
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
    vendor_name = "binance"
    mode = "download"
    version = args.universe
    if version == "all":
        currency_pairs = fetch_active_symbols()
    else:
        universe = imvcounun.get_vendor_universe(
            vendor_name, mode, version=version
        )
        currency_pairs = universe["binance"]
    # Create Binance API client.
    api_client = imvbdeapcl.BinanceAPIClient(args.secret_name)
    data_type = args.bid_ask_data_type
    # Create S3 path to dump data.
    s3_bucket = hs3.get_s3_bucket_from_stage(stage=args.stage)
    today_date = pd.Timestamp(args.stop_date)
    start_date = pd.Timestamp(args.start_date)
    end_date = start_date + pd.Timedelta(days=7)
    currency_pairs_failed = []
    while end_date < today_date:
        failed = download_data_to_s3_with_exception(
            api_client,
            start_date,
            end_date,
            currency_pairs,
            data_type=data_type,
            s3_bucket=s3_bucket,
            incremental=args.incremental,
            save_path_prefix=args.save_path_prefix,
        )
        start_date = end_date + pd.Timedelta(days=1)
        end_date = start_date + pd.Timedelta(days=7)
        currency_pairs_failed.extend(failed)
    # Download remaining dates.
    end_date = today_date - pd.Timedelta(days=1)
    failed = download_data_to_s3_with_exception(
        api_client,
        start_date,
        end_date,
        currency_pairs,
        data_type=data_type,
        s3_bucket=s3_bucket,
        incremental=args.incremental,
        save_path_prefix=args.save_path_prefix,
    )
    currency_pairs_failed.extend(failed)
    if currency_pairs_failed:
        msg = f"The following currency pairs failed: {currency_pairs_failed}"
        hdbg.dfatal(msg)


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _run(args)


if __name__ == "__main__":
    _main(_parse())
