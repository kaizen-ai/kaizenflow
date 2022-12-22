"""
Import as:

import surrentum_infra_sandbox.validate as sinsaval
"""

import abc
from typing import Any, List


class QaCheck(abc.ABC):
    """
    Represent a single QA check executed on one or more datasets.

    E.g.,

    - check that OHLCV data is in the right format (e.g., timestamps are not missing, L < O, L < H, V != 0)
    - check that two data dataframes from different providers are compatible (e.g., the error is less than 1%)
    """

    def __init__(self) -> None:
        self._status: str = "Check has not been executed."

    @abc.abstractmethod
    def check(self, datasets: List, *args: Any) -> bool:
        """
        Perform an individual QA data validation check on DataFrame(s).

        # TODO(Juraj): by accepting list we can have a unified interface for single dataset
        #  and cross dataset checks but we not sure yet.
        :param datasets: List of one or more datasets (e.g. DataFrames)
        :return: True if the check is passed, False otherwise
        """
        ...

    def get_status(self) -> str:
        """
        Return a formatted status message.
        """
        return f"{self.__class__.__name__}: {self._status}"


class DatasetValidator(abc.ABC):

    # TODO(Juraj): Similiarly as above, by accepting list of signatures
    # we can have a unified interface for single dataset and cross dataset checks.
    def __init__(self, qa_checks: List[QaCheck]) -> None:
        self.qa_checks = qa_checks

    @abc.abstractmethod
    def run_all_checks(self, datasets: List, *args: Any) -> None:
        """
        Run all checks.

        :param datasets: List of one or more datasets (e.g. DataFrames)
        """
        ...
