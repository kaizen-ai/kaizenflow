#!/usr/bin/env python
import abc
import logging
from typing import Any, List

import pandas as pd

import helpers.hdbg as hdbg


class QaCheck(abc.ABC):
    def __init__(self) -> None:
        self._status: str = "Check has not been executed."

    def __str__(self) -> str:
        """
        Return a description of a check.
        """
        return f"{self.__class__}: {self._status}"

    @abc.abstractmethod
    def check(self, dataframes: List[pd.DataFrame], *args: Any) -> bool:
        """
        Perform an individual QA data validation check on DataFrame(s).

        # TODO(Juraj): by accepting list we can have a unified interface for single dataset
        #  and cross dataset checks but we not sure yet.
        :param data: List of DataFrames
        :return: True if the check is passed, False otherwise
        """
        ...

    def get_status(self) -> str:
        return self._status


class DatasetValidator(abc.ABC):

    # TODO(Juraj): Similiarly as above, by accepting list of signatures
    # we can have a unified interface for single dataset and cross dataset checks.
    def __init__(
        self, dataset_signatures: List[str], qa_checks: List[QaCheck]
    ) -> None:
        # The class will hold a reference to a raw data client instance
        #  which takes care of loading the data.
        self.dataset_signatures = dataset_signatures
        self.qa_checks = qa_checks

    @abc.abstractmethod
    def run_all_checks(self, logger: logging.Logger, *args) -> None:
        """
        Run all checks.
        """
        ...


class EmptyDatasetCheck(QaCheck):
    def check(self, dataframes: List[pd.DataFrame], *args: Any) -> bool:
        """
        Assert a DataFrame is not empty.
        """
        hdbg.dassert_eq(len(dataframes), 1)
        is_empty = dataframes[0].empty
        self._status = "FAILED: Dataset is empty" if is_empty else "PASSED"
        return is_empty


class SingleDatasetValidator(DatasetValidator):
    def run_all_checks(self, logger: logging.Logger) -> None:
        error_msgs: List[str] = []
        for qa_check in self.qa_checks:
            if qa_check.check(dataset_signatures[0]):
                logger.info(check.get_status)
            else:
                error_msgs.append(check.get_status)
        if error_msgs:
            error_msg = "\n".join(error_msgs)
            hdbg.dfatal(error_msg)
