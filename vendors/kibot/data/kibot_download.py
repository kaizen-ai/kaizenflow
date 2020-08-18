#!/usr/bin/env python
"""
Download data from kibot.com, compress each file, upload it to S3.

# Start from scratch and process all datasets
> futures_1mins/kibot_download.py --delete_s3_dir

# Process only specific dataset
> futures_1mins/kibot_download.py --dataset all_stocks_1min

# Process several datasets
> futures_1mins/kibot_download.py --dataset all_stocks_1min --dataset all_stocks_dailys

# Debug
> futures_1mins/kibot_download.py --serial -v DEBUG
"""

import argparse
import getpass
import logging
import os
import re

import bs4
import joblib
import numpy as np
import pandas as pd
import requests
import tqdm

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.s3 as hs3
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)
_KIBOT_ENDPOINT = "http://www.kibot.com/"
_KIBOT_MY_ACCOUNT = _KIBOT_ENDPOINT + "account.aspx"
_SOURCE_DIR_NAME = "source_data"
_CONVERTED_DIR_NAME = "converted_data"
_DATASETS = [
    "all_stocks_1min",
    "all_stocks_unadjusted_1min",
    "all_stocks_daily",
    "all_stocks_unadjusted_daily",
    "all_etfs_1min",
    "all_etfs_unadjusted_1min",
    "all_etfs_daily",
    "all_etfs_unadjusted_daily",
    "all_forex_pairs_1min",
    "all_forex_pairs_daily",
    "all_futures_contracts_1min",
    "all_futures_contracts_daily",
    "all_futures_continuous_contracts_tick",
    "all_futures_continuous_contracts_1min",
    "all_futures_continuous_contracts_daily",
]

# #############################################################################


def _log_in(
        page_url: str,
        username: str,
        password: str,
        requests_session: requests.Session,
) -> bool:
    """
    Make a login request to my account page and return the result.

    :param page_url: URL to the my account page
    :param username: actual username
    :param password: real password
    :param requests_session: current requests session to preserve cookies
    :return: boolean for operation result
    """
    _LOG.info("Requesting page '%s'", page_url)
    page_response = requests_session.get(page_url)
    page_content = str(page_response.content, "utf-8")
    soup = bs4.BeautifulSoup(page_content, "html.parser")
    view_state_input = soup.find("input", attrs={"name": "__VIEWSTATE"})
    event_validation_input = soup.find(
        "input", attrs={"name": "__EVENTVALIDATION"}
    )
    data = {
        "__VIEWSTATE": view_state_input.attrs.get('value'),
        "__EVENTVALIDATION": event_validation_input.attrs.get('value'),
        "ctl00$Content$LoginView1$Login1$UserName": username,
        "ctl00$Content$LoginView1$Login1$Password": password,
        "ctl00$Content$LoginView1$Login1$RememberMe": "on",
        "ctl00$Content$LoginView1$Login1$LoginButton": "  Log In  ",
    }
    _LOG.info("Sending login request to page '%s'", page_url)
    _LOG.debug("Request data is %s", data)
    login_response = requests_session.post(page_url, data=data, allow_redirects=False)
    if login_response.status_code == 302:
        return True
    _LOG.error("Unexpected response from the login request")
    return False


def _download_page(
        page_file_path: str, page_url: str, requests_session: requests.Session,
) -> str:
    """
    Download html file by URL and store under specific name in data directory.

    :param page_file_path: path of the file
    :param page_url: URL from where to download
    :param requests_session: current requests session to preserve cookies
    :return: contents of the page
    """
    _LOG.info("Requesting page '%s'", page_url)
    page_response = requests_session.get(page_url)
    _LOG.info("Storing page to '%s'", page_file_path)
    with open(page_file_path, "w+b") as f:
        f.write(page_response.content)
    page_content = str(page_response.content, "utf-8")
    return page_content


def _clean_dataset_name(dataset: str) -> str:
    """
    Clean up a dataset name for ease future reference.
    E.g., the dataset `1. All Stocks 1min on 9/29/2019` becomes `all_stocks_1min`.

    :param dataset: input dataset name to process
    :return: cleaned dataset name
    """
    clean_dataset = dataset.lower()
    clean_dataset = re.sub(r"^\d+.", "", clean_dataset)
    clean_dataset = re.sub(r"\s+on.*$", "", clean_dataset)
    clean_dataset = re.sub(r"\s+", "_", clean_dataset)
    clean_dataset = clean_dataset.strip("_")
    return clean_dataset


def _extract_dataset_links(src_file: str) -> pd.DataFrame:
    """
    Retrieve a table with datasets and corresponding page links.

    :param src_file: html file with the my account page
    :return: DataFrame with dataset names and corresponding page links
    """
    html = io_.from_file(src_file)
    soup = bs4.BeautifulSoup(html, "html.parser")
    # Get last table.
    table = soup.findAll("table")[-1]
    df = pd.read_html(str(table))[0]
    df.columns = ["dataset", "link"]
    cols = [
        np.where(dataset.has_attr("href"), dataset.get("href"), "no link")
        for dataset in table.find_all("a")
    ]
    # Reset first column with links.
    df.link = [str(c) for c in cols]
    # Clean up dataset names for future ease reference.
    df.dataset = df.dataset.apply(_clean_dataset_name)
    return df


def _extract_payload_links(src_file: str) -> pd.DataFrame:
    """
    Extract a table from dataset html page.

    :param src_file: path to dataset html file page
    :return: DataFrame with the list of series with Symbol and Link columns
    """
    html = io_.from_file(src_file)
    # Find HTML that refers a required table.
    _, table_start, rest = html.partition('<table class="ms-classic4-main">')
    table, table_end, _ = rest.partition('</table>')
    # Replace all anchors with their href attributes.
    table = re.sub('<a.*?href="(.*?)">(.*?)</a>', '\\1', table)
    # Construct back the table.
    table = table_start + table + table_end
    df = pd.read_html(table)[0]
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    return df


def _download_payload_page(local_dir: str, aws_dir: str, row: pd.Series) -> bool:
    """
    Store CSV payload for specific Symbol in S3.

    :param local_dir: local directory with the data
    :param aws_dir: remove directory on S3 server
    :param row: series with Symbol and Link columns
    :return: boolean for operation result
    """
    aws_file = aws_dir + "/"
    aws_file += "%s.csv.gz" % row["Symbol"]
    # Check if S3 file exists.
    rc = si.system("aws s3 ls " + aws_file, abort_on_error=False)
    exists = not rc
    _LOG.debug("%s -> exists=%s", aws_file, exists)
    if exists:
        _LOG.info("%s -> skip", aws_file)
        return False
    # Download data.
    local_file = "%s/%s.csv" % (local_dir, row["Symbol"])
    # --compression=gzip
    cmd = "wget '%s' -O %s" % (row["Link"], local_file)
    si.system(cmd)
    dst_file = local_file.replace(".csv", ".csv.gz")
    cmd = "gzip %s -c >%s" % (local_file, dst_file)
    si.system(cmd)
    # Delete csv file.
    cmd = "rm -f %s" % local_file
    si.system(cmd)
    #
    local_file = dst_file
    # Copy to s3.
    cmd = "aws s3 cp %s s3://%s" % (local_file, aws_file)
    si.system(cmd)
    # Delete local file.
    cmd = "rm -f %s" % local_file
    si.system(cmd)
    return True


class PasswordPrompter:
    DEFAULT = "Prompt if not specified"

    def __init__(self, value):
        if value == self.DEFAULT:
            value = getpass.getpass("Password: ")
        self.value = value

    def __str__(self):
        return self.value


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-u", "--username", help="Specify username", default=getpass.getuser()
    )
    parser.add_argument(
        "-p",
        "--password",
        type=PasswordPrompter,
        help="Specify password",
        default=PasswordPrompter.DEFAULT,
    )
    parser.add_argument(
        "-t",
        "--tmp_dir",
        type=str,
        nargs="?",
        help="Directory to store temporary data",
        default="tmp.kibot_downloader",
    )
    parser.add_argument(
        "-d",
        "--dataset",
        type=str,
        help="Proceed with a specific dataset or all datasets at once if omitted",
        choices=_DATASETS,
        action="append",
        default=None,
    )
    parser.add_argument(
        "-s", "--serial", action="store_true", help="Download data serially"
    )
    parser.add_argument(
        "--delete_s3_dir",
        action="store_true",
        help="Delete the S3 dir before starting uploading",
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    requests_session = requests.Session()
    io_.create_dir(args.tmp_dir, incremental=True)
    source_dir = os.path.join(args.tmp_dir, _SOURCE_DIR_NAME)
    converted_dir = os.path.join(args.tmp_dir, _CONVERTED_DIR_NAME)
    io_.create_dir(source_dir, incremental=True)
    io_.create_dir(converted_dir, incremental=True)
    login_result = _log_in(
        _KIBOT_MY_ACCOUNT, args.username, str(args.password), requests_session
    )
    if not login_result:
        # Unable to login
        return
    my_account_file = os.path.join(source_dir, "my_account.html")
    # Download my account html page.
    if not os.path.exists(my_account_file):
        _LOG.warning("Missing %s: downloading it", my_account_file)
        _download_page(my_account_file, _KIBOT_MY_ACCOUNT, requests_session)
    # Parse and convert my account page.
    dataset_links_csv_file = os.path.join(
        converted_dir, "dataset_links.csv"
    )
    _LOG.warning("Parsing %s", my_account_file)
    dataset_links_df = _extract_dataset_links(
        os.path.join(source_dir, "my_account.html")
    )
    dataset_links_df.to_csv(dataset_links_csv_file)
    datasets_to_proceed = args.dataset or _DATASETS
    # Process a dataset.
    for dataset in datasets_to_proceed:
        dataset_html_file = os.path.join(source_dir, f"{dataset}.html")
        dataset_csv_file = os.path.join(converted_dir, f"{dataset}.csv")
        # Download dataset's html page.
        if not os.path.exists(dataset_html_file):
            _LOG.warning("Missing %s: downloading it", dataset_html_file)
            [link_to_html_page] = dataset_links_df.loc[
                dataset_links_df.dataset == dataset
                ].link.values
            _download_page(dataset_html_file, link_to_html_page, requests_session)
        # Parse and convert dataset's html page.
        _LOG.warning("Parsing %s", dataset_html_file)
        dataset_df = _extract_payload_links(dataset_html_file)
        dataset_df.to_csv(dataset_csv_file)
        _LOG.info("Number of files to download: %s", dataset_df.shape[0])
        _LOG.info(dataset_df.head())
        dataset_dir = os.path.join(converted_dir, dataset)
        io_.create_dir(dataset_dir, incremental=True)
        aws_dir = os.path.join(hs3.get_path(), "kibot", dataset)
        if args.delete_s3_dir:
            _LOG.warning("Deleting s3 file %s", aws_dir)
            cmd = "aws s3 rm --recursive %s" % aws_dir
            si.system(cmd)
        # Download data.
        if not args.serial:
            joblib.Parallel(n_jobs=5, verbose=10)(
                joblib.delayed(_download_payload_page)(
                    dataset_dir, aws_dir, row
                )
                for i, row in dataset_df.iterrows()
            )
        else:
            for _, row in tqdm.tqdm(dataset_df.iterrows(), total=len(dataset_df)):
                _download_payload_page(dataset_dir, aws_dir, row)


if __name__ == "__main__":
    _main(_parse())
