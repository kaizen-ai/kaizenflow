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
import re
import zipfile

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm.autonotebook import tqdm

import helpers.csv as csv
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)

_WEBSITE = "http://firstratedata.com"
_ZIPPED_DST_DIR = "/data/firstrate_zipped/"
_UNZIPPED_DST_DIR = "/data/firstrate_unzipped/"
_PQ_DST_DIR = "data/firstrate_pq"


class FileURL:
    """
    A container for file urls.

    :param timezone: The timezone of the dataset
    :param url: file url
    :param category: the category of an equity
    """

    def __init__(self, url, timezone, category, col_names, path=""):
        self.url = url
        self.timezone = timezone
        self.category = category
        self.col_names = col_names
        self.path = path


class RawDataDownloader:
    """
    Get all category urls from a website, then walk them one by one and
    extract all download links and timezones. Save the files from the
    download links to the category directories. The timezone
    information will be appended to each file name as a suffix.

    :param website: the website url
    :param dst_dir: destination directory
    """

    def __init__(self, website, dst_dir):
        self.website = website
        self.dst_dir = dst_dir
        self.file_urls = []
        self.path_object_dict = {}

    def execute(self):
        """
        Get all category urls from a website, then walk them one by one and
        extract all download links and timezones. Save the files from the
        download links to the category directories. The timezone
        information will be appended to each file name as a suffix.
        """
        _LOG.info("Collecting the links")
        all_urls = self.walk_get_all_urls()
        _LOG.info("Downloading the files")
        path_object = {}
        for url_object in tqdm(all_urls):
            self._download_url_to_path(url_object)
            path_object[url_object.file_path] = url_object
        self.path_object_dict = path_object

    def walk_get_all_urls(self):
        """
        Get all category urls from a website, then walk them one by one and
        extract all download links and timezones into FileURL objects.

        :return: a list of FileURL objects
        """
        categories_urls = self._get_categories_urls()
        url_objects = []
        for category_url in tqdm(categories_urls):
            url_objects_category = self._walk_html_with_links_to_download_links(
                category_url
            )
            url_objects.extend(url_objects_category)
        self.file_urls = url_objects
        return url_objects

    @staticmethod
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

    def _add_website_to_url(self, url_part):
        """
        If the first part of the url is cut, add it.

        :param url_part: The cut url
        :return: Full url
        """
        if "http" in url_part:
            full_url = url_part
        else:
            full_url = f"{self.website}{url_part}"
        return full_url

    @staticmethod
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
        full_urls = [
            RawDataDownloader._add_website_to_url(part_url, website)
            for part_url in part_urls
        ]
        return full_urls

    def _get_card_bodies_urls(self, soup):
        """
        Finds all "card-body" attributes in BeautifulSoup and extracts urls
        from them.

        :param soup: BeautifulSoup object
        :return: [[full_urls]] - a list of urls for each card body
        """
        card_bodies = soup.find_all(attrs="card-body")
        urls_for_cards = []
        for card_body in card_bodies:
            full_urls = self._get_urls(card_body, self.website)
            urls_for_cards.append(full_urls)
        return urls_for_cards

    @staticmethod
    def _extract_timezones(card_body):
        """
        Extract timezones from a BeautifulSoup object.
        Timezones are located in the card-body attribute

        :param card_body: BeautifulSoup object
        :return: list of timezones
        """
        time_zones = []
        for label in card_body.select("label"):
            if label.string == "Time Zone":
                time_zones.append(
                    label.next_element.next_element.next_element.string
                )
        return time_zones

    @staticmethod
    def _extract_col_names(card_body):
        columns = []
        for label in card_body.select("label"):
            if label.string == "Format : ":
                cols_label = label.next_element.next_element.next_element.string
                cols_list = cols_label.split("|")
                cols_list = list(map(lambda x: x.strip(), cols_list))
                columns.append(cols_list)
        return columns

    def _get_download_links_and_tzs(self, card_bodies, category):
        """
        Extract timezones and download links from a list of BeautifulSoup
        objects (in firstrate the links and timezones are located in
        card_bodies attributes).

        :param card_bodies: BeautifulSoup objects
        :param category: category of the url (commodity, stock_A-D, etc.)
            This will be used later to save files to different directories.
        :return: list ot FileURL objects (containing timezone, url and
            category)
        """
        url_objects = []
        for card_body in card_bodies:
            tzs = self._extract_timezones(card_body)
            col_names = self._extract_col_names(card_body)
            urls = self._get_urls(card_body, self.website)
            for tz, col_name_list, url in zip(tzs, col_names, urls):
                furl = FileURL(url, tz, category, col_name_list)
                url_objects.append(furl)
        return url_objects

    @staticmethod
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

    def _download_url_to_path(self, url_object):
        """
        Construct a path from the contents of the FileURL object by adding
        a category directory and appending timezone as a suffix to the
        file name from the url.

        :param url_object: FileURL object
        """
        category_dir_path = os.path.join(self.dst_dir, url_object.category)
        if not os.path.isdir(category_dir_path):
            os.mkdir(path=category_dir_path)
            _LOG.info(f"Created {category_dir_path} directory")
        file_name = url_object.url.split("/")[-1]
        file_name = f"_{url_object.timezone}.".join(file_name.rsplit("."))
        file_path = os.path.join(category_dir_path, file_name)
        url_object.file_path = file_path
        self._download_file(url_object.url, url_object.file_path)

    def _walk_html_with_links_to_download_links(self, html_url):
        """
        Go to each link from a (category) page and get all links and
        timezones found there.

        :param html_url: a (category) url
        :return: list of FileURL objects
        """
        category = "_".join(html_url.split("/")[4:])
        soup = self._request_soup(html_url)
        full_urls = np.array(self._get_card_bodies_urls(soup)).flatten()
        url_objects_for_urls = []
        for full_url in tqdm(full_urls):
            dataset_soup = self._request_soup(full_url)
            if dataset_soup is None:
                _LOG.warning(
                    f"No files found at {full_url}, request returned None"
                )
            else:
                card_bodies = dataset_soup.find_all(attrs="card-body")
                if len(card_bodies) == 0:
                    _LOG.warning(f"No links were found for {full_url}")
                url_objects = self._get_download_links_and_tzs(
                    card_bodies, category
                )
                url_objects_for_urls.extend(url_objects)
        all_urls = url_objects_for_urls
        return all_urls

    def _get_categories_urls(self):
        r"""
        Get all nav-links containing 'it/' from the website.

        :return: list of full urls
        """
        soup = self._request_soup(self.website)
        nav_links = soup.find_all(attrs="nav-link")
        hrefs = list(map(lambda x: x["href"], nav_links))
        hrefs_categories = [href for href in hrefs if "it/" in href]
        hrefs_categories_urls = [
            self._add_website_to_url(part_url) for part_url in hrefs_categories
        ]
        return hrefs_categories_urls


class ZipCSVCombiner:
    def __init__(self, url_object: FileURL, output_path: str):
        self.url_object = url_object
        self.output_path = output_path

    def execute(self):
        df = self._read_zipped_csvs()
        df = self._process_df(df)
        df.to_csv(self.output_path, index=0)

    def _read_zipped_csvs(self):
        dfs = []
        with zipfile.ZipFile(self.url_object.path) as zf:
            for csv_path in zf.namelist():
                with zf.open(csv_path) as zc:
                    df_part = pd.read_csv(zc, sep=",", header=None)
                dfs.append(df_part)
        df = pd.concat(dfs)
        return df

    @staticmethod
    def _add_col_names(df, col_names):
        if len(col_names) < len(df.columns):
            col_names.extend([None] * (len(df.columns) - len(col_names)))
        df.columns = col_names
        return df

    @staticmethod
    def _add_timestamp_col(df):
        cols = df.columns
        if "yyyymmdd" in re.sub("[./: ]", "", cols[0]).lower():
            if "hh" not in cols[0]:
                if "hhmm" in cols[1]:
                    df["timestamp"] = df[cols[0]].astype(str) + " " + df[cols[1]]
                    df["timestamp"] = pd.to_datetime(df["timestamp"])
                else:
                    df["timestamp"] = pd.to_datetime(df[cols[0]], format="%Y%m%d")
            else:
                df["timestamp"] = pd.to_datetime(df[cols[0]])
        else:
            _LOG.warning("No timestamp column was created")
        return df

    def _process_df(self, df):
        df = self._add_col_names(df, self.url_object.col_names)
        df = self._add_timestamp_col(df)
        if "timestamp" in df.columns:
            df["timestamp"] = df["timestamp"].tz_localize(
                self.url_object.timezone
            )
            df.sort_values("timestamp", inplace=True)
        else:
            df.sort_values(df.columns[0], inplace=True)
        return df


def combine_zipped_csvs(input_dir, path_object_dict, dst_dir):
    """
    Combine zipped csvs in firstrate directory. Add column names to the
    csvs, add timestamp column and localize it.

    :param input_dir: firstrate directory with categories
    :param path_object_dict: path_object_dict attribute of the
        RawDataDownloader
    :param dst_dir: destination directory
    """
    for category_dir in tqdm(os.listdir(input_dir)):
        category_dir_input_path = os.path.join(input_dir, category_dir)
        category_dir_dst_path = os.path.join(dst_dir, category_dir)
        if not os.path.isdir(category_dir_dst_path):
            os.mkdir(path=category_dir_dst_path)
            _LOG.info(f"Created {category_dir_dst_path} directory")
        for zip_path in tqdm(os.listdir(category_dir_input_path)):
            url_object = path_object_dict[zip_path]
            csv_name = os.path.splitext(zip_path.split("/")[-1])[0] + ".csv"
            csv_path = os.path.join(category_dir_dst_path, csv_name)
            zcc = ZipCSVCombiner(url_object, csv_path)
            zcc.execute()


def save_to_parquet(input_dir, dst_dir):
    """
    Save csv files divided by categories into parquet

    :param input_dir: input directory with categories
    :param dst_dir: destination directory
    """
    for category_dir in tqdm(os.listdir(input_dir)):
        category_dir_input_path = os.path.join(input_dir, category_dir)
        category_dir_dst_path = os.path.join(dst_dir, category_dir)
        if not os.path.isdir(category_dir_dst_path):
            os.mkdir(path=category_dir_dst_path)
            _LOG.info(f"Created {category_dir_dst_path} directory")
        csv.convert_csv_dir_to_pq_dir(
            category_dir_input_path, category_dir_dst_path
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--zipped_dst_dir",
        required=False,
        action="store",
        default=_ZIPPED_DST_DIR,
        type=str,
    )
    parser.add_argument(
        "--unzipped_dst_dir",
        required=False,
        action="store",
        default=_UNZIPPED_DST_DIR,
        type=str,
    )
    parser.add_argument(
        "--pq_dst_dir",
        required=False,
        action="store",
        default=_PQ_DST_DIR,
        type=str,
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

    rdd = RawDataDownloader(args.website, args.zipped_dst_dir)
    rdd.execute()

    combine_zipped_csvs(
        args.zipped_dst_dir, rdd.path_object_dict, args.unzipped_dst_dir
    )

    save_to_parquet(args.unzipped_dst_dir, args.pq_dst_dir)
