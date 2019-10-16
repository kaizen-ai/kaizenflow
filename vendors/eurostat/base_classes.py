import os
from typing import Tuple, Generator, Dict, Union
import bs4
import requests
import generalized_filler.base_classes as gf_bc
from tqdm import tqdm


class EurostatFileFiller(gf_bc.FileFiller):
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
        Return (True, msg) if dst_dir doesn't exist, otherwise returns (False, msg).
        """
        if not os.path.exists(self.dst_dir):
            status = True
            msg = "OK"
            return status, msg
        status = False
        msg = "Folder exist."
        return status, msg

    def data_reader(self) -> Generator[Dict[str, Union[str, bytes]], None, None]:
        r = requests.get(self.root_url)
        data = r.content
        soup = bs4.BeautifulSoup(data, 'html5lib')
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
