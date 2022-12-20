"""
Import as:

import surrentum_infra_sandbox.download as sinsadow
"""

import abc
from typing import Any


class RawData:
    """
    Wrapper class for downloaded data to support uniform interface.
    """

    def __init__(self, data: Any) -> None:
        self.data = data

    def get_data(self) -> Any:
        """
        Download data from a desired source.

        :param start_timestamp: start of the download period (context differs based on data type)
        :param end_timestamp: end of the download period (context differs based on data type)
        :return
        """
        return self.data


class DataDownloader(abc.ABC):
    """
    Abstract class for downloading raw data from a third party.
    """

    @abc.abstractmethod
    def download(
        self, *, start_timestamp=None, end_timestamp=None, **kwargs: Any
    ) -> RawData:
        """
        Download data from a desired source.

        :param start_timestamp: start of the download period (context differs based on data type).
        If None, start with the earliest possible data.
        :param end_timestamp: end of the download period (context differs based on data type)
        If None, download up to the latest possible data.
        :return: raw downloaded dataset
        """
        ...
