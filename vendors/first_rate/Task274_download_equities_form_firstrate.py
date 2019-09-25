import argparse
import logging
import os

import numpy as np
import requests
from bs4 import BeautifulSoup
from tqdm.autonotebook import tqdm

_LOG = logging.getLogger(__name__)

WEBSITE = "http://firstratedata.com"
DST_DIR = '/data/firstrate/'


# TODO: first get all urls from the website, then save them
# TODO: rename url, website, html
# TODO: add proper directory management
# TODO: add parallelization (but in the way we won't get blocked)


class FileURL:

    def __init__(self, timezone, url):
        self.timezone = timezone
        self.url = url

    def __eq__(self, other):
        if not isinstance(other, FileURL):
            return NotImplemented
        return self.timezone == other.timezone and self.url == other.url


def request_soup(html_url):
    req_res = requests.get(html_url)
    if req_res.status_code == 200:
        soup = BeautifulSoup(req_res.content, "lxml")
    else:
        _LOG.warning(f'Request status code is {req_res.status_code}')
        soup = None
    return soup


def add_website_to_url(url_part, website):
    if 'http' in url_part:
        full_url = url_part
    else:
        full_url = f'{website}{url_part}'
    return full_url


def get_urls(soup, website):
    hrefs = soup.find_all('a')
    part_urls = [href['href'] for href in hrefs]
    full_urls = [add_website_to_url(part_url, website) for part_url in
                 part_urls]
    return full_urls


def get_card_bodies_urls(soup, website):
    card_bodies = soup.find_all(attrs="card-body")
    urls_for_cards = []
    for card_body in card_bodies:
        full_urls = get_urls(card_body, website)
        urls_for_cards.append(full_urls)
    return urls_for_cards


def extract_timezone(dataset_soup):
    time_zones = []
    for label in dataset_soup.select('label'):
        if label.string == 'Time Zone':
            time_zones.append(
                label.next_element.next_element.next_element.string)
    return time_zones


def get_download_links_and_tzs(card_bodies, webiste):
    tzs_urls = []
    for card_body in card_bodies:
        tzs = extract_timezone(card_body)
        urls = get_urls(card_body, webiste)
        tzs_urls.append((tzs, urls))
    return tzs_urls


def get_download_links_and_tzs_v2(card_bodies, webiste):
    url_objects = []
    for card_body in card_bodies:
        tzs = extract_timezone(card_body)
        urls = get_urls(card_body, webiste)
        # url_objects.append((tzs, urls))
        for tz, url in zip(tzs, urls):
            url_objects.append(FileURL(timezone=tz, url=url))
    return url_objects


def download_file(url, file_path):
    with requests.get(url, stream=True) as r:
        with open(file_path, 'wb') as fout:
            fout.write(r.content)
    _LOG.info(f'Saved {url} to {file_path}')


def download_urls_to_path(tzs_urls, dst_dir):
    for tzs, urls in tzs_urls:
        for i, url in enumerate(urls):
            tz = tzs[i]
            file_name = url.split('/')[-1]
            file_name = f'_{tz}.'.join(file_name.rsplit('.'))
            file_path = os.path.join(dst_dir, file_name)
            download_file(url, file_path)


def download_url_to_path_v2(file_url_object, dst_dir):
    file_name = file_url_object.url.split('/')[-1]
    file_name = f'_{file_url_object.timezone}.'.join(file_name.rsplit('.'))
    file_path = os.path.join(dst_dir, file_name)
    download_file(file_url_object.url, file_path)


def walk_html_with_links_to_download_links(html_url, website, dst_dir):
    soup = request_soup(html_url)
    full_urls = np.array(get_card_bodies_urls(soup, website)).flatten()
    for full_url in tqdm(full_urls):
        dataset_soup = request_soup(full_url)
        if dataset_soup is None:
            _LOG.warning(
                f'No files loaded for {full_url}, request returned None')
        else:
            card_bodies = soup.find_all(attrs="card-body")
            if len(card_bodies) == 0:
                _LOG.warning(f'No links were found for {full_url}')
            download_links_tzs = get_download_links_and_tzs(card_bodies,
                                                            website)
            download_urls_to_path(download_links_tzs, dst_dir=dst_dir)


def walk_html_with_links_to_download_links_v2(html_url, website):
    soup = request_soup(html_url)
    full_urls = np.array(get_card_bodies_urls(soup, website)).flatten()
    url_objects_for_urls = []
    for full_url in tqdm(full_urls):
        dataset_soup = request_soup(full_url)
        if dataset_soup is None:
            _LOG.warning(
                f'No files loaded for {full_url}, request returned None')
        else:
            card_bodies = soup.find_all(attrs="card-body")
            if len(card_bodies) == 0:
                _LOG.warning(f'No links were found for {full_url}')
            url_objects = get_download_links_and_tzs_v2(card_bodies,
                                                            website)
            url_objects_for_urls.append(url_objects)
    all_urls = np.array(url_objects_for_urls).flatten()
    return all_urls


def get_categories_urls(website):
    soup = request_soup(website)
    nav_links = soup.find_all(attrs="nav-link")
    hrefs = list(map(lambda x: x['href'], nav_links))
    hrefs_categories = [href for href in hrefs if 'it/' in href]
    hrefs_categories_urls = [add_website_to_url(part_url, website) for part_url
                             in hrefs_categories]
    return hrefs_categories_urls

#TODO: determine url category before downloading


def walk_get_all_urls(website):
    categories_urls = get_categories_urls(website=website)
    url_objects_categories = []
    for category_url in tqdm(categories_urls):
        category_dir = '_'.join(category_url.split('/')[4:])
        url_objects_category = walk_html_with_links_to_download_links_v2(html_url=category_url,
                                               website=WEBSITE)
        url_objects_categories.append(url_objects_category)
    all_urls = np.array(url_objects_categories).flatten()
    return all_urls


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dst_dir', required=False, action='store', type=str)
    parser.add_argument('--website', required=False, action='store', type=str)
    parser.add_argument("-v", dest="log_level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR",
                                 "CRITICAL"], help="Set the logging level", )
    args = parser.parse_args()
    if args.dst_dir:
        dst_dir = args.dst_dir
    else:
        _LOG.info(f'Using default {DST_DIR} dst_dir')
        dst_dir = DST_DIR
    if args.website:
        website = args.website
    else:
        _LOG.info(f'Using default {WEBSITE} website')
        website = WEBSITE
    categories_urls = get_categories_urls(website=website)
    for category_url in tqdm(categories_urls):
        category_dir = '_'.join(category_url.split('/')[4:])
        category_dir_path = os.path.join(dst_dir, category_dir)
        if not os.path.isdir(category_dir_path):
            os.mkdir(path=category_dir_path)
            _LOG.info('Created %s directory' % category_dir_path)
        walk_html_with_links_to_download_links(html_url=category_url,
                                               website=WEBSITE,
                                               dst_dir=category_dir_path)
        _LOG.info(f'Saved all files from {category_url}')
