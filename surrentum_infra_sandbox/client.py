"""
Import as:

import surrentum_infra_sandbox.client as sinsacli
"""

import abc
from typing import Any, Optional

import pandas as pd


class DataClient(abc.ABC):
    """
    Abstract class for loading data from disk into main memory.
    """

    @abc.abstractmethod
    def load(
        self,
        dataset_signature: str,
        start_timestamp: Optional[pd.Timestamp] = None,
        end_timestamp: Optional[pd.Timestamp] = None,
        **kwargs: Any
    ) -> Any:
        """
        Load data from a desired source and a desired time interval.

        The invariant for loading from a specified time interval is:
        [start_timestamp, end_timestamp), i.e., start_timestamp is included,
        end_timestamp is excluded.

        :param dataset_signature: signature of the dataset to load
        :param start_timestamp: beginning of the time period to load (the context
            differs based on data type). If None, start with the earliest saved
            data.
        :param end_timestamp: end of the time period to load (the context differs
            based on data type). If None, download up to the latest saved data.
        :return: loaded data
        """
        ...
