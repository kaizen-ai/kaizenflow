"""
Import as:

import surrentum_infra_sandbox.client as sinsacli
"""

import abc
from typing import Any


class DataClient(abc.ABC):
    """
    Abstract class for loading data from disk into main memory.
    """

    @abc.abstractmethod
    def load(
        self,
        dataset_signature: str,
        start_timestamp=None,
        end_timestamp=None,
        **kwargs: Any
    ) -> Any:
        """
        Load data from a desired source in a desired time interval.

        The invariant for loading from a specified time interval is:
        [start_timestamp, end_timestamp) -> start_timestamp included, end_timestamp excluded.

        :param dataset_signature: signature of the dataset to load
        :param start_timestamp: beginning of the time period to load 
         (context differs based on data type).
        If None, start with the earliest saved data.
        :param end_timestamp: end of the time period to load 
        (context differs based on data type)
        If None, download up to the latest saved data.
        :return: loaded data
        """
        ...
