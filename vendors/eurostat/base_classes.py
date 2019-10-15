import os
from typing import Tuple, Generator, Dict, Union
from bs4 import BeautifulSoup
import requests
from generalized_filler.base_classes import FileFiller
from tqdm import tqdm


class EurostatFileSaver(FileFiller):
    """
    Saves data using root_url.

    Search all files from root_url using pattern *.tsv.gz
    """

    def __init__(self,
                 filler_version: str):
        super().__init__(filler_version=filler_version)
        self.root_url = self.optional['root_url']

    def paranoid_handler(self) -> Tuple[bool, str]:
        """
        Folder shouldn't exists or must be empty.
        :return:
        """
        if not os.path.exists(self.dst_dir):
            return True, "OK"
        if not os.listdir(self.dst_dir):
            return True, "OK, folder exists but it's empty."
        return False, "Folder exists and not empty."

    def data_reader(self) -> Generator[Dict[str, Union[str, bytes]], None, None]:
        r = requests.get(self.root_url)
        data = r.content
        soup = BeautifulSoup(data, 'html5lib')
        soup_a = soup.find_all('a')
        list_a = [i for i in filter(lambda x: x.text == 'Download', soup_a)]
        for link in tqdm(list_a):
            url = link.get('href')
            file_name = url.split('%2F')[-1]
            rr = requests.get(url)
            yield {"data": rr.content, 'file_name': file_name}

    def fill(self, **kwargs):
        for data in self.data_reader():
            self.fill_data(**data)
