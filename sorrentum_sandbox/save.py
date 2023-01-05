"""
Import as:

import sorrentum_sandbox.save as sinsasav
"""

import abc
from typing import Any

import sorrentum_sandbox.download as sinsadow


class DataSaver(abc.ABC):
    """
    Abstract class for saving data to a persistent storage such as PostgreSQL /
    S3.
    """

    @abc.abstractmethod
    def save(self, data: sinsadow.RawData, *args: Any, **kwargs: Any) -> None:
        """
        Save data to a persistent storage.

        :param data: data to persist
        """
        ...