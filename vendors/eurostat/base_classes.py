import os
from abc import ABC, abstractmethod
from typing import Tuple, Generator, Dict, Union, List

import bs4
import requests
from tqdm import tqdm

import amp.helpers.dbg as dbg
import etl.generalized_filler.base_classes as gf_bc


class EurostatFileFillerV1(ABC):
    """
    Interface for Eurostat downloader.
    """
    @abstractmethod
    def get_source_page(self) -> bs4.BeautifulSoup:
        """
        Get the source from self.root_url page.

        :return: bs4.BeautifulSoup
        """
        pass

    @abstractmethod
    def get_links(self) -> List[str]:
        """
        Find all the links that we interested for downloading.

        :return: List[str]
        """
        pass


class EurostatDirFiller(EurostatFileFillerV1, gf_bc.FileFiller):
    """
    Saves data using FileFiller backend.
    Search all files from root_url.

    runnable script:
    etl.generalized_filler/filler.py

    usage:
    python etl.generalized_filler/filler.py \
    --filler_version SOME_FILLER_VERSION \
    --class_version v2


    - SOME_FILLER_VERSION can be found in filler_versions.py.
    On the same module level where places module with current class.
    """

    def __init__(self,
                 filler_version: str):
        super().__init__(filler_version=filler_version)
        self.root_url = self.optional['root_url']

    def paranoid_handler(self) -> Tuple[bool, str]:
        """
        Return (True, msg) if dst_dir doesn't exist, otherwise returns (False, msg).
        """
        if not os.path.exists(self.dst_dir):
            status = True
            msg = "OK"
            return status, msg
        status = False
        msg = "Folder exist."
        return status, msg

    def get_source_page(self) -> bs4.BeautifulSoup:
        """
        Get the source from self.root_url page.

        :return: bs4.BeautifulSoup
        """
        r = requests.get(self.root_url)
        dbg.dassert_eq(
            200,
            r.status_code,
            msg=f'Request status "200" expected. We have {r.status_code}.')
        data = r.content
        soup = bs4.BeautifulSoup(data, 'html5lib')
        return soup

    def get_links(self) -> List[str]:
        """
        Find all the links in the main page called "Download".

        :return: List[str]
        """
        soup = self.get_source_page()
        soup_a = soup.find_all('a')
        dbg.dassert_lte(1, len(soup_a))
        # We expect that all files that we interest in can be downloaded using
        # hrefs.
        # Text of each href must be equal to 'Download'.
        # Check root_url to see how page looks like.
        list_a = [i for i in filter(lambda x: x.text == 'Download', soup_a)]
        # url example:
        # https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&downfile=data%2Faact_ali01.tsv.gz
        return list_a

    def data_reader(self) -> Generator[
        Dict[str, Union[str, bytes]], None, None]:
        """
        Find all the links in the main page called "Download" and retrieve the
        file which is a `.tsv.gz` file.
        """
        links_list = self.get_links()
        for link in tqdm(links_list):
            url = link.get('href')
            file_name = url.split('%2F')[-1]
            rr = requests.get(url)
            yield {"data": rr.content, 'file_name': file_name}

    def fill(self, **kwargs):
        for data in self.data_reader():
            self.fill_data(**data)
