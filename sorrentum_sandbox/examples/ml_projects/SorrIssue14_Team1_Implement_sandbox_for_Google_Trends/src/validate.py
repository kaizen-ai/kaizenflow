"""
QA pipeline for Google Trends .

Import as:
Import google_trends.src.validate as validator
"""

from typing import Any, List
import pandas as pd
import common.validate as sinsaval


class DenormalizedDatasetCheck(sinsaval.QaCheck):
    """
    Assert that a DataFrame is not normalized.
    """

    def check(self, dataframes: List[pd.DataFrame], *args: Any) -> bool:

        for dataframe in dataframes:
            if len(dataframe[dataframe["Frequency"] < 0]) >= 1:
                self._status = "FAILED: search frequencies less than 0"
                return False

            elif len(dataframe[dataframe["Frequency"] > 100]) >= 1:
                self._status = "FAILED: search frequencies greater than 100"
                return False

            else:
                self._status = "PASSED"
                return True


