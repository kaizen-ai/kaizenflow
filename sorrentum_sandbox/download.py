"""
Import as:

import sorrentum_sandbox.download as sinsadow
"""

import abc
from typing import Any, Optional

import pandas as pd
# #############################################################################
# RawData
# #############################################################################


# TODO(gp): Not sure it's worth to have a wrapper here.
class RawData:
    """
    Wrapper class for downloaded data to support uniform interface.
    """

    def __init__(self, data: Any) -> None:
        self.data = data

    def get_data(self) -> Any:
        """
        Download data from a desired source.
        """
        return self.data


# #############################################################################
# DataDownloader
# #############################################################################


class DataDownloader(abc.ABC):
    """
    Abstract class for downloading raw data from a third party.
    """

    @abc.abstractmethod
    def download(
        self,
        *,
        start_timestamp: Optional[pd.Timestamp] = None,
        end_timestamp: Optional[pd.Timestamp] = None,
        **kwargs: Any
    ) -> RawData:
        """
        Download data from a desired source.

        The invariant for downloading in a specified time interval is
        [start_timestamp, end_timestamp), i.e., start_timestamp is included,
        end_timestamp is excluded.

        The context differs based on data type.

        :param start_timestamp: start of the download period. If `None`, start
            with the earliest possible data.
        :param end_timestamp: end of the download period. If `None`, download
            up to the latest possible data.
        :return: raw downloaded dataset
        """
        ...
