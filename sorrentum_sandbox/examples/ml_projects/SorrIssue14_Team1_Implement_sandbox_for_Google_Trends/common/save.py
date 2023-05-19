"""
Import as:

import sorrentum_sandbox.common.save as sinsasav
"""

import abc
import pandas as pd
import common.download as sinsadow


# #############################################################################
# DataSaver
# #############################################################################


class DataSaver(abc.ABC):
    """
    Save data to a persistent storage (e.g., PostgreSQL, S3).
    """

    @abc.abstractmethod
    def save(self, data: pd.DataFrame) -> None:
        """
        Save data to persistent storage.

        :param data: data to save
        """
        ...
