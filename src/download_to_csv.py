#!/usr/bin/env python
"""
Download google trends data and save it as CSV locally.
"""

import logging
import os
from typing import Any
import helpers.hdbg as hdbg
import src.download as sisebido
import common.save as sinsasav
import pandas as pd

_LOG = logging.getLogger(__name__)


class CsvDataFrameSaver(sinsasav.DataSaver):
    """
    Class for saving pandas DataFrame as CSV to a local filesystem at desired
    location.
    """

    def __init__(self, target_dir: str) -> None:
        """
        Constructor.

        :param target_dir: path to save data to.
        """
        self.target_dir = target_dir

    def save(self, data: pd.DataFrame, **kwargs: Any) -> None:
        """
        storing a DataFrame to CSV.

        :param data: data to persists into CSV
        """
        hdbg.dassert_isinstance(data, pd.DataFrame, "Only DataFrame is supported.")
        signature = "cleaned_data_1.csv"
        target_path = os.path.join(self.target_dir, signature)
        data.to_csv(target_path, index=False)


# #############################################################################

if __name__ == "__main__":
    downloader = sisebido.OhlcvRestApiDownloader()
    raw_data = downloader.download(topic="monsoon")
    # Save data as CSV.
    saver = CsvDataFrameSaver("../files")
    saver.save(raw_data)
