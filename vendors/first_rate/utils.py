#!/usr/bin/env python
r"""
Download equity data from the http://firstratedata.com.

Usage example:
> python oil/utils/Task274_download_equities_form_firstrate.py \
  --dst_dir /data/firstrate/ \
  --website http://firstratedata.com
"""
import argparse
import logging
import os

import numpy as np
import requests
from bs4 import BeautifulSoup
from tqdm.autonotebook import tqdm

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)

_WEBSITE = "http://firstratedata.com"
_DST_DIR = "/data/firstrate/"


class FileURL:
    def __init__(self, timezone, url, category):
        self.timezone = timezone
        self.url = url
        self.category = category


def _request_soup(html_url):
    """
    Download html content of the page to BeautifulSoup.

    :param html_url: html url
    :return: BeautifulSoup object if the request status code was 200,
        otherwise None
    """
    req_res = requests.get(html_url)
    if req_res.status_code == 200:
        soup = BeautifulSoup(req_res.content, "lxml")
    else:
        _LOG.warning(
            f"For {html_url} request status code is {req_res.status_code}"
        )
        soup = None
    return soup


def _add_website_to_url(url_part, website):
    """
    If the first part of the url is cut, add it.

    :param url_part: The cut url
    :param website: The first part of the url
    :return: Full url
    """
    if "http" in url_part:
        full_url = url_part
    else:
        full_url = f"{website}{url_part}"
    return full_url


def _get_urls(soup, website):
    """
    Get all urls from a page.

    :param soup: BeautifulSoup object where the urls need to be found
    :param website: if the urls' first parts are cut,
        add website to them
    :return: list of detected urls
    """
    hrefs = soup.find_all("a")
    part_urls = [href["href"] for href in hrefs]
    full_urls = [_add_website_to_url(part_url, website) for part_url in part_urls]
    return full_urls


def _get_card_bodies_urls(soup, website):
    """
    Finds all "card-body" attributes in BeautifulSoup and extracts urls
    from them.

    :param soup: BeautifulSoup object
    :param website: If the found urls are missing their first parts,
        this website will be added to them
    :return: [[full_urls]] - a list of urls for each card body
    """
    card_bodies = soup.find_all(attrs="card-body")
    urls_for_cards = []
    for card_body in card_bodies:
        full_urls = _get_urls(card_body, website)
        urls_for_cards.append(full_urls)
    return urls_for_cards


def _extract_timezones(dataset_soup):
    """
    Extract timezones from a BeautifulSoup object.

    :param dataset_soup: BeautifulSoup object
    :return: list of timezones
    """
    time_zones = []
    for label in dataset_soup.select("label"):
        if label.string == "Time Zone":
            time_zones.append(label.next_element.next_element.next_element.string)
    return time_zones


def get_download_links_and_tzs_v2(card_bodies, website, category):
    """
    Extract timezones and download links from a list of BeautifulSoup
    objects (in firstrate the links and timezones are located in
    card_bodies attributes).

    :param card_bodies: BeautifulSoup objects
    :param website: If the urls' first parts are cut, this website
        will be added to them
    :param category: category of the url (commodity, stock_A-D, etc.)
        This will be used later to save files to different directories.
    :return: list ot FileURL objects (containing timezone, url and
        category)
    """
    url_objects = []
    for card_body in card_bodies:
        tzs = _extract_timezones(card_body)
        urls = _get_urls(card_body, website)
        for tz, url in zip(tzs, urls):
            furl = FileURL(tz, url, category)
            url_objects.append(furl)
    return url_objects


def _download_file(url, dst_path):
    """
    Download file from url to path.

    :param url: url of the file
    :param dst_path: destination path
    """
    with requests.get(url, stream=True) as r:
        with open(dst_path, "wb") as fout:
            fout.write(r.content)
    _LOG.info(f"Saved {url} to {dst_path}")


def _download_url_to_path_v2(url_object, dst_dir):
    """
    Construct a path from the contents of the FileURL object by adding
    a category directory and appending timezone as a suffix to the
    file name from the url.

    :param url_object: FileURL object
    :param dst_dir: a destination directory
    """
    category_dir_path = os.path.join(dst_dir, url_object.category)
    if not os.path.isdir(category_dir_path):
        os.mkdir(path=category_dir_path)
        _LOG.info(f"Created {category_dir_path} directory")
    file_name = url_object.url.split("/")[-1]
    file_name = f"_{url_object.timezone}.".join(file_name.rsplit("."))
    file_path = os.path.join(category_dir_path, file_name)
    _download_file(url_object.url, file_path)


def _walk_html_with_links_to_download_links_v2(html_url, website):
    category = "_".join(html_url.split("/")[4:])
    soup = _request_soup(html_url)
    full_urls = np.array(_get_card_bodies_urls(soup, website)).flatten()
    url_objects_for_urls = []
    for full_url in tqdm(full_urls):
        dataset_soup = _request_soup(full_url)
        if dataset_soup is None:
            _LOG.warning(f"No files found at {full_url}, request returned None")
        else:
            card_bodies = dataset_soup.find_all(attrs="card-body")
            if len(card_bodies) == 0:
                _LOG.warning(f"No links were found for {full_url}")
            url_objects = get_download_links_and_tzs_v2(
                card_bodies, website, category
            )
            url_objects_for_urls.extend(url_objects)
    all_urls = url_objects_for_urls
    return all_urls


def _get_categories_urls(website):
    r"""
    Get all nav-links containing 'it/' from the website.

    :param website: url of the website
    :return: list of full urls
    """
    soup = _request_soup(website)
    nav_links = soup.find_all(attrs="nav-link")
    hrefs = list(map(lambda x: x["href"], nav_links))
    hrefs_categories = [href for href in hrefs if "it/" in href]
    hrefs_categories_urls = [
        _add_website_to_url(part_url, website) for part_url in hrefs_categories
    ]
    return hrefs_categories_urls


def walk_get_all_urls(website):
    """
    Get all category urls from a website, then walk them one by one and
    extract all download links and timezones into FileURL objects.

    :param website: url of the website
    :return: a list of FileURL objects
    """
    categories_urls = _get_categories_urls(website)
    url_objects = []
    for category_url in tqdm(categories_urls):
        url_objects_category = _walk_html_with_links_to_download_links_v2(
            category_url, website
        )
        url_objects.extend(url_objects_category)
    return url_objects


def main(website, dst_dir):
    """
    Get all category urls from a website, then walk them one by one and
    extract all download links and timezones. Save the files from the
    download links to the category directories. The timezone
    information will be appended to each file name as a suffix.

    :param website: the website url
    :param dst_dir: destination directory
    """
    _LOG.info("Collecting the links")
    all_urls = walk_get_all_urls(website)
    _LOG.info("Downloading the files")
    for url_object in tqdm(all_urls):
        _download_url_to_path_v2(url_object, dst_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--dst_dir", required=False, action="store", default=_DST_DIR, type=str
    )
    parser.add_argument(
        "--website", required=False, action="store", default=_WEBSITE, type=str
    )
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    args = parser.parse_args()
    dbg.init_logger(args.log_level)

    main(args.website, args.dst_dir)
