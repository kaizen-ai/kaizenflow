r"""
Import as:

import vendors.first_rate.utils as fru


Download equity data from the http://firstratedata.com.

- Save the data as zipped csvs for each equity to one of the
  [commodity, crypto, fx, index, stock_A-D, stock_E-I, stock_J-N,
  stock_O-R, stock_S-Z] category directories.
- Combine zipped csvs for each equity, add "timestamp" column and
  column names. Save as csv to corresponding category directories
- Save csvs to parquet (divided by category)
"""

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
import helpers.io_ as io_

_LOG = logging.getLogger(__name__)


class _FileURL:
    """
    A container for file urls.
    """

    def __init__(self, url, timezone, category, col_names, path=""):
        """
        :param url: file url
        :param timezone: The timezone from the dataset description on the
            website
        :param category: the navigation category in which a file appears on
            the FirstRate website. The categories are:
            [commodity, crypto, fx, index, stock_A-D, stock_E-I, stock_J-N,
            stock_O-R, stock_S-Z]
        :param col_names: Column names from the "Format" field of the
            dataset description on the FirstRate website
        :param path: A path to which this file is saved as zipped csv
        """
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
    """

    def __init__(self, website, dst_dir, max_num_files):
        """
        :param website: the website url
        :param dst_dir: destination directory
        """
        self.website = website
        self.dst_dir = dst_dir
        self.file_urls = []
        self.path_object_dict = {}
        if max_num_files is not None:
            _LOG.warning("Limiting number of files to %s", max_num_files)
            dbg.dassert_lte(1, max_num_files)
        self.max_num_files = max_num_files

    def execute(self):
        _LOG.info("Collecting the links")
        all_urls = self._walk_get_all_urls()
        if self.max_num_files is not None:
            all_urls = all_urls[: self.max_num_files]
        _LOG.info("Downloading the %d files", len(all_urls))
        path_object = {}
        for url_object in tqdm(all_urls):
            self._download_url_to_path(url_object)
            path_object[url_object.path] = url_object
        self.path_object_dict = path_object

    def _walk_get_all_urls(self):
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
                "For %s request status code is %s", html_url, req_res.status_code
            )
            soup = None
        return soup

    @staticmethod
    def _add_website_to_url(url_part, website):
        """
        If the first part of the url is cut, add it.

        :param url_part: The cut url
        :param website The first part of the url
        :return: Full url
        """
        if not url_part.startswith("http"):
            full_url = f"{website}{url_part}"
        else:
            full_url = url_part
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
        objects (in first_rate the links and timezones are located in
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
                if url.split("/")[-2] != "purchase":
                    furl = _FileURL(url, tz, category, col_name_list)
                    url_objects.append(furl)
                else:
                    _LOG.warning(
                        "%s is a purchase link, not downloading" "its contents",
                        url,
                    )
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
        _LOG.debug("Saved %s to %s", url, dst_path)

    def _download_url_to_path(self, url_object):
        """
        Construct a path from the contents of the FileURL object by adding
        a category directory and appending timezone as a suffix to the
        file name from the url.

        :param url_object: FileURL object
        """
        category_dir_path = os.path.join(self.dst_dir, url_object.category)
        io_.create_dir(category_dir_path, incremental=True)
        file_name = os.path.basename(url_object.url)
        file_name = f"_{url_object.timezone}.".join(file_name.rsplit("."))
        file_path = os.path.join(category_dir_path, file_name)
        url_object.path = file_path
        self._download_file(url_object.url, url_object.path)

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
                    "No files found at %s, request returned None", full_url
                )
            else:
                card_bodies = dataset_soup.find_all(attrs="card-body")
                if len(card_bodies) == 0:
                    _LOG.warning("No links were found for %s", full_url)
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
            self._add_website_to_url(part_url, self.website)
            for part_url in hrefs_categories
        ]
        return hrefs_categories_urls


class _ZipCSVCombiner:
    """
    - Combine csvs from a zip file
    - add "timestamp" column
    - localize that column to the timezone parsed from the FirstRate
      website
    - add column names (parsed from the FirstRate website)
    - save as csv
    """

    def __init__(self, url_object: _FileURL, output_path: str):
        """
        :param url_object: _FileURL object
        :param output_path: destination path for the csv
        """
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
        processed_dfs = []
        for df in dfs:
            df = self._process_datetime_cols(df)
            processed_dfs.append(df)
        df = pd.concat(processed_dfs)
        return df

    @staticmethod
    def _process_datetime_cols(df):
        first_col = 0
        second_col = 1
        if isinstance(df.iloc[0, 0], (int, np.int64)):
            first_col = "date"
        elif isinstance(df.iloc[0, 0], str):
            # Remove any of the '\', '.', '/', ':', '-', ' ' symbols
            # from the date/datetime col. If the length of the cleaned
            # element is 8, it means it is a date column. If the
            # length >= 11, the column contains both date and time.
            chars_regexp = r"[\./:\s-]"
            backslash_regexp = r"\\"
            without_symbols = re.sub(chars_regexp, "", df.iloc[0, 0])
            without_symbols = re.sub(backslash_regexp, "", without_symbols)
            if len(without_symbols) == 8:
                first_col = "date"
            elif len(without_symbols) >= 11:
                first_col = "datetime"
        if first_col == "date":
            if isinstance(df.iloc[0, 1], str):
                if df.iloc[0, 1][-3] == ":":
                    second_col = "time"
        df.rename(columns={0: first_col, 1: second_col}, inplace=True)

        if first_col == "date":
            if second_col == "time":
                df["timestamp"] = df.iloc[:, 0].astype(str) + " " + df.iloc[:, 1]
                df.drop(columns=["time"], inplace=True)
            else:
                df["timestamp"] = df.iloc[:, 0]
                df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y%m%d")
            df.drop(columns=["date"], inplace=True)
        elif first_col == "datetime":
            df["timestamp"] = df.iloc[:, 0]
            df.drop(columns=["datetime"], inplace=True)
        else:
            _LOG.warning("Timestamp column was not found")
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df[["timestamp"] + list(df.columns.drop("timestamp"))]
            df.columns = ["timestamp"] + list(range(df.shape[1] - 1))
        return df

    @staticmethod
    def _add_col_names(df, col_names):
        col_names = list(map(lambda x: x.lower(), col_names))
        col_names = ["timestamp"] + [
            col
            for col in col_names
            if ("date" not in str(col) and "time" not in str(col))
        ]
        if len(col_names) < len(df.columns):
            col_names.extend([""] * (len(df.columns) - len(col_names)))
        elif len(col_names) > len(df.columns):
            col_names = col_names[: df.shape[1]]
        df.columns = col_names
        return df

    def _process_df(self, df):
        _TIMEZONES = {"UTC": "UTC", "EST": "US/Eastern"}
        df = self._add_col_names(df, self.url_object.col_names)
        if "timestamp" in df.columns:
            df["timestamp"] = df["timestamp"].dt.tz_localize(
                _TIMEZONES[self.url_object.timezone]
            )
            df.sort_values("timestamp", inplace=True)
        else:
            df.sort_values(df.columns[0], inplace=True)
        return df


class MultipleZipCSVCombiner:
    """
    Combine zipped csvs in first_rate directory. Add column names to the
    csvs, add timestamp column and localize it.
    """

    def __init__(self, input_dir, path_object_dict, dst_dir):
        """
        :param input_dir: first_rate directory with categories
        :param path_object_dict: path_object_dict attribute of the
            RawDataDownloader
        :param dst_dir: destination directory
        """
        self.input_dir = input_dir
        self.path_object_dict = path_object_dict
        self.dst_dir = dst_dir

    def execute(self):
        _LOG.info("Combining zipped csvs into one")
        for category_dir in tqdm(os.listdir(self.input_dir)):
            category_dir_input_path = os.path.join(self.input_dir, category_dir)
            category_dir_dst_path = os.path.join(self.dst_dir, category_dir)
            io_.create_dir(category_dir_dst_path, incremental=True)
            for zip_name in tqdm(os.listdir(category_dir_input_path)):
                # Combine csvs from the zip file, process them and save.
                zip_path = os.path.join(category_dir_input_path, zip_name)
                url_object = self.path_object_dict[zip_path]
                csv_name = os.path.splitext(zip_name)[0] + ".csv"
                csv_path = os.path.join(category_dir_dst_path, csv_name)
                zcc = _ZipCSVCombiner(url_object, csv_path)
                zcc.execute()


class CsvToParquetConverter:
    """
    Save csv files divided by categories into parquet
    """

    def __init__(self, input_dir, dst_dir, to_datetime, timestamp_col):
        """
        :param input_dir: input directory with categories
        :param dst_dir: destination directory
        :param to_datetime: whether to transform timestamp column to
            datetime format
        :param timestamp_col: the name of the timestamp column
        """
        self.input_dir = input_dir
        self.dst_dir = dst_dir
        self._to_datetime = to_datetime
        self._timestamp_col = timestamp_col

    def execute(self):
        _LOG.info("Saving to parquet")
        for category_dir in tqdm(os.listdir(self.input_dir)):
            category_dir_input_path = os.path.join(self.input_dir, category_dir)
            category_dir_dst_path = os.path.join(self.dst_dir, category_dir)
            io_.create_dir(category_dir_dst_path, incremental=True)
            if self._to_datetime:
                normalizer = self._ts_to_datetime
            else:
                normalizer = None
            csv.convert_csv_dir_to_pq_dir(
                category_dir_input_path,
                category_dir_dst_path,
                header="infer",
                normalizer=normalizer,
            )

    def _ts_to_datetime(self, df):
        df[self._timestamp_col] = pd.to_datetime(df[self._timestamp_col])
        return df
