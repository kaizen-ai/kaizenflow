"""
Script to download historical data from Cryptodatadownload.
"""

import argparse
import logging
import os
import urllib
from typing import List

import bs4
import pandas as pd

import helpers.datetime_ as hdatet
import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.parser as prsr

_LOG = logging.getLogger(__name__)

WEBSITE_PREFIX = "https://www.cryptodatadownload.com/data/"


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
        help="Cryptodatadownload name of the exchange to download data for, e.g. 'binance'",
    )
    parser.add_argument(
        "--timeframe",
        action="store",
        required=True,
        type=str,
        help="Timeframe of the data to load. Possible values: 'minute', 'hourly', 'daily'",
    )
    parser.add_argument("--incremental", action="store_true")
    parser = prsr.add_verbosity_arg(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Get html contents of webpage as string.
    page_content = get_download_page(args.exchange_id)
    # Parse download links from the page.
    download_links = get_download_links(page_content, args.timeframe)
    # Set destination directory name and create it.
    dst_dir = args.dir_name
    hio.create_dir(dst_dir, incremental=args.incremental)
    _LOG.info("Downloading %s links" % len(download_links))
    for link in download_links:
        df = pd.read_csv(link)
        _LOG.info("Downloaded %s" % link)
        timestamp = hdatet.get_timestamp("ET")
        # Construct filename.
        if exchange_id.lower() not in dst_dir.lower():
            dst_dir = os.path.join(dst_dir, exchange_id + "/")
        hio.create_dir(dst_dir, incremental=True)
        # Select filename from url.
        orig_filename = link.rsplit("/", 1)[-1]
        # Construct new name with timestamp.
        filename = os.path.join(dst_dir, f"{args.timestamp}_{orig_filename}")
        _LOG.info("Saved to %s" % filename)
        df.to_csv(filename, index=False)
    _LOG.info("Download finished")
    return None


def get_download_page(exchange_id: str) -> str:
    """
    Get html contents of webpage as string.
    """
    # Construct url and load.
    download_url = WEBSITE_PREFIX + exchange_id
    u1 = urllib.request.urlopen(download_url)
    page_content = []
    for line in u1:
        page_content.append(line.decode("utf-8"))
    page_content = "".join(page_content)
    return page_content


def get_download_links(
    download_page_content: str,
    timeframe: str,
) -> List[str]:
    """
    Parse download links from the page.

    Example of download link tag:

    ```
    <a href="/cdd/Binance_BTCUSDT_minute.csv"> [Minute]</a>
    ```

    :param download_page_content: html content of the page with links
    :param timeframe: timeframe of the data to load. Possible values:
        'minute', 'hourly', 'daily'.
    :return: list of download links
    """
    soup = bs4.BeautifulSoup(download_page_content)
    download_tags = soup.find_all(
        lambda tag: tag.name == "a" and timeframe in tag.text.lower()
    )
    download_links = [
        urllib.parse.urljoin(WEBSITE_PREFIX, tag["href"]) for tag in download_tags
    ]
    return download_links


if __name__ == "__main__":
    _main(_parse())
