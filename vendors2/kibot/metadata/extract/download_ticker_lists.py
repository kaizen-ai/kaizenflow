import argparse
import logging
import os
import sys
import urllib.parse as urlprs
from typing import List

import bs4
import requests

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr
import helpers.s3 as hs3
import helpers.system_interaction as si

# TODO(amr): move common configs between data & metadata to
# `vendors2.kibot.config`
import vendors2.kibot.data.config as config
import vendors2.kibot.data.extract.download as download

_LOG = logging.getLogger(__name__)


SUB_DIR = os.path.join("metadata", "raw", "ticker_list")
S3_PREFIX = os.path.join(config.S3_PREFIX, SUB_DIR)

# TODO(amr): reuse requests setup from extract/download.py.


def _extract_file_url_from_historical_page(page_url: str) -> str:
    """Extract txt file url from historical page.

    Example historical page url:
    http://www.kibot.com/Historical_Data/Top_50_Stocks_Historical_Intraday_Data.aspx.

    :return: file url, from example: http://www.kibot.com/Files/2/Top_50_Stocks_Intraday.txt
    """
    response = requests.get(url=page_url)
    page_soup = bs4.BeautifulSoup(response.content, "html.parser")
    file_link = page_soup.select('p a[href^="../Files"]')[0].attrs["href"]
    return urlprs.urljoin(page_url, file_link)


def _extract_ticker_page_urls() -> List[str]:
    """Extract ticker page links from http://www.kibot.com/buy.aspx.

    :return: list of ticker page links, for example:
        ['http://www.kibot.com/Historical_Data/Top_50_Stocks_Historical_Intraday_Data.aspx', ...]
    """
    response = requests.get(url=config.ENDPOINT + "buy.aspx")
    soup = bs4.BeautifulSoup(response.content, "html.parser")

    available_data_sets = soup.select("strong a")
    return [config.ENDPOINT + s.attrs["href"] for s in available_data_sets]


# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--tmp_dir",
        type=str,
        nargs="?",
        help="Directory to store temporary data",
        default="tmp.kibot_downloader",
    )
    parser.add_argument(
        "--no_incremental",
        action="store_true",
        help="Clean the local directories",
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> int:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Create dirs.
    incremental = not args.no_incremental
    io_.create_dir(args.tmp_dir, incremental=incremental)

    page_urls = _extract_ticker_page_urls()
    _LOG.info("Found %s historical page urls", len(page_urls))

    # TODO(amr): reuse parallelization logic in extract/download.py.

    for url in page_urls:
        _LOG.info("Processing historical page: %s", url)
        file_url = _extract_file_url_from_historical_page(page_url=url)
        _LOG.info("Extracted file url: %s", file_url)

        file_name = os.path.basename(urlprs.urlparse(file_url).path)
        # TODO(amr): is cleaning the file name necessary? if so, let's move this
        # function to a more common place.
        file_name = download.DatasetListExtractor._clean_dataset_name(  # pylint: disable=protected-access
            file_name
        )
        _LOG.info("Cleaned up file name: %s", file_name)

        # Download file.
        response = requests.get(file_url)
        dbg.dassert_eq(response.status_code, 200)
        file_path = os.path.join(args.tmp_dir, SUB_DIR, file_name)
        io_.to_file(file_name=file_path, lines=str(response.content, "utf-8"))
        _LOG.info("Downloaded file to: %s", file_path)

        # Save to s3.
        aws_path = os.path.join(S3_PREFIX, file_name)
        hs3.check_valid_s3_path(aws_path)
        # TODO(amr): create hs3.copy() helper.
        cmd = "aws s3 cp %s %s" % (file_path, aws_path)
        si.system(cmd)
        _LOG.info("Uploaded file to s3: %s", aws_path)

    return 0


if __name__ == "__main__":
    sys.exit(_main(_parse()))
