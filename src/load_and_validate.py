#!/usr/bin/env python
"""
Load and validate data within a specified time period from a CSV file.
"""

import logging
import os
from typing import Any, Optional

import pandas as pd

import common.client as sinsacli
import common.validate as sinsaval
import src.validate as sisebiva

_LOG = logging.getLogger(__name__)


# #############################################################################
# CsvClient
# #############################################################################


class CsvClient(sinsacli.DataClient):
    """
    Class for loading CSV data from local filesystem into main memory.
    """

    def __init__(self, source_dir: str) -> None:
        """
        Constructor.

        :param source_dir: path to source data from.
        """
        self.source_dir = source_dir

    def load(
        self,
        dataset_signature: str,
        *,
        start_timestamp: Optional[pd.Timestamp] = None,
        end_timestamp: Optional[pd.Timestamp] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Load CSV data specified by a unique signature.
        """
        dataset_signature += ".csv"
        source_path = os.path.join(self.source_dir, dataset_signature)
        data = pd.read_csv(source_path)

        return data


# #############################################################################
# Script.
# #############################################################################


if __name__ == "__main__":
    csv_client = CsvClient("../files")
    data = csv_client.load("cleaned_data")

    denormalized_dataset_check = sisebiva.DenormalizedDatasetCheck()

    dataset_validator = sinsaval.SingleDatasetValidator(
        [denormalized_dataset_check]
    )

    dataset_validator.run_all_checks([data])
