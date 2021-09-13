#!/usr/bin/env python

"""
Script to download historical data from CryptoDataDownload.
"""

import argparse
import logging
import os
import ssl
import urllib.request
from typing import List

import bs4
import pandas as pd

import helpers.datetime_ as hdatet
import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.parser as prsr

_LOG = logging.getLogger(__name__)

_WEBSITE_PREFIX = "https://www.cryptodatadownload.com/data/"


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--dir_name",
        action="store",
        required=True,
        type=str,
        help="Directory name for the files to be stored",
    )
    parser.add_argument(
        "--exchange_id",
        action="store",
        required=True,
        type=str,
        help="CryptoDataDownload name of the exchange to download data for, e.g. 'binance'",
    )
    parser.add_argument(
        "--timeframe",
        action="store",
        required=True,
        type=str,
        choices=["minute", "hourly", "daily"],
        help="Timeframe of the data to load",
    )
    parser.add_argument("--incremental", action="store_true")
    parser = prsr.add_verbosity_arg(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    # Disable default certificate verification by python in order to
    # avoid error when running this script in a notebook.
    ssl._create_default_https_context = ssl._create_unverified_context
    # Set parser arguments.
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Get HTML contents of webpage as string.
    page_content = _get_download_page(args.exchange_id)
    # Parse download links from the page.
    download_links = _get_download_links(page_content, args.timeframe)
    # Create destination directory.
    hio.create_dir(args.dir_name, incremental=args.incremental)
    _LOG.info("Downloading %s links", len(download_links))
    for link in download_links:
        df = pd.read_csv(link)
        _LOG.info("Downloaded %s", link)
        timestamp = hdatet.get_timestamp("ET")
        # Construct filename.
        dst_dir = os.path.join(args.dir_name, args.exchange_id)
        hio.create_dir(dst_dir, incremental=True)
        # Select filename from URL.
        orig_filename = link.rsplit("/", 1)[-1]
        # Construct new name with timestamp.
        filename = os.path.join(dst_dir, f"{timestamp}_{orig_filename}")
        _LOG.info("Saved to %s", filename)
        df.to_csv(filename, index=False)
    _LOG.info("Download finished")


def _get_download_page(exchange_id: str) -> str:
    """
    Get HTML contents of webpage as string.
    """
    # Construct URL and load.
    download_url = _WEBSITE_PREFIX + exchange_id
    u1 = urllib.request.urlopen(download_url)
    page_content = []
    for line in u1:
        page_content.append(line.decode("utf-8"))
    page_content = "".join(page_content)
    return page_content


def _get_download_links(
    download_page_content: str,
    timeframe: str,
) -> List[str]:
    """
    Parse download links from the page.

    Example of download link tag:

    ```
    <a href="/cdd/Binance_BTCUSDT_minute.csv"> [Minute]</a>
    ```

    :param download_page_content: HTML content of the page with links
    :param timeframe: timeframe of the data to load. Possible values:
        'minute', 'hourly', 'daily'.
    :return: list of download links
    """
    soup = bs4.BeautifulSoup(download_page_content)
    download_tags = soup.find_all(
        lambda tag: tag.name == "a" and timeframe in tag.text.lower()
    )
    download_links = [
        urllib.parse.urljoin(_WEBSITE_PREFIX, tag["href"]) for tag in download_tags
    ]
    return download_links


if __name__ == "__main__":
    _main(_parse())
