from typing import List, Dict
import glob
import os
import pandas as pd
import gzip
from generalized_reader.base_clases import FileReader


class EurostatReader(FileReader):
    def __init__(self,
                 filler_version: str):
        super().__init__(filler_version=filler_version)
        self.src_dir = self.filler_class.get_dst_dir(filler_version=filler_version)

    def get_filenames(self, prefixes: List[str]) -> List[str]:
        output = []
        for prefix in prefixes:
            output.extend(glob.glob(os.path.join(self.src_dir, prefix + '*')))
        return output

    def read_data(self, prefixes: List[str]) -> Dict[str, pd.DataFrame]:
        """
        Reading data in dict structure
        "file_name" - full file name
        "data" - loaded df
        :param prefixes: list of strings or ["*"] to get all the data,
        file with all prefixes explained https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=table_of_contents_en.pdf
        :return: list of dicts
        """
        output = dict()
        file_names = self.get_filenames(prefixes)
        for file_name in file_names:
            with gzip.open(file_name) as f:
                df: pd.DataFrame = pd.read_csv(f, sep='\t')
                output[file_name] = df
        return output
