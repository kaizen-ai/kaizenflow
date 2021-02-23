"""
Converts data stored in S3 to SQL.
"""

import logging
import abc
from typing import Any, Callable, List, Optional


import pandas as pd
import vendors_amp.common.data.types as vkdtyp

_LOG = logging.getLogger(__name__)


class AbstractS3ToSqlTransformer(abc.ABC):

    @classmethod
    def transform(
        cls,
        df: pd.DataFrame,
        trade_symbol_id: int,
        frequency: vkdtyp.Frequency,
    ) -> pd.DataFrame:
        """
        Transform data loaded from S3 to load to SQL.
   
        :param df: dataframe with data from S3
        :param trade_symbol_id: symbol id in SQL database
        :param frequency: dataframe frequency
        :return: processed dataframe
        """
