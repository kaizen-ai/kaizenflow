import glob
import gzip
import logging
import os
from typing import List, Dict

import pandas as pd

import generalized_reader.base_clases as gr_bc

_LOG = logging.getLogger(__name__)


class EurostatReader(gr_bc.FileReader):
    def __init__(self,
                 filler_version: str):
        super().__init__(filler_version=filler_version)
        self.src_dir = self.filler_class.get_dst_dir(filler_version=filler_version)

    def get_filenames(self, prefixes: List[str]) -> List[str]:
        output = []
        for prefix in prefixes:
            # Get the files in the source dir starting with `prefix`.
            glob_pattern = os.path.join(self.src_dir, prefix + '*')
            file_names = glob.glob(glob_pattern)
            _LOG.debug("In %s found %d filenames (%s)", glob_pattern, len(file_names), file_names)
            # Accumulate.
            output.extend(file_names)
        return output

    def read_data(self, prefixes: List[str]) -> Dict[str, pd.DataFrame]:
        """
        Reading data in dict structure.
        File with all prefixes explained:
         https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=table_of_contents_en.pdf

        :param prefixes: list of strings or ["*"] to get all the data,
        :return: dict, {"/some/file/full/path.ext": pd.DataFrame}
        """
        output = dict()
        file_names = self.get_filenames(prefixes)
        for file_name in file_names:
            with gzip.open(file_name) as f:
                df: pd.DataFrame = pd.read_csv(f, sep='\t')
                output[file_name] = df
        return output
