"""
Import as:

import core.stats_computer as cstats
"""

import collections
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

import core.finance as cfinan
import core.signal_processing as csigna
import core.statistics as cstati
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class StatsComputer:
    """
    Allows to get particular piece of stats instead of the whole stats table.
    
    Available methods:
        - summarize_time_index_info
        - ...
    """
    def summarize_time_index_info(
        self, 
        srs: pd.Series,
        oos_start: Optional[Union[str, pd.Timestamp]] = None,
        mode: str = "ins",
    ) -> pd.Series:
        return self._calculate_series_stats(
            srs=srs,
            stats_name="summarize_time_index_info",
            oos_start=oos_start,
            mode=mode,
        )
    
    @staticmethod
    def _calculate_series_stats(
        srs: pd.Series, 
        stats_name: str,
        oos_start: Optional[Union[str, pd.Timestamp]] = None,
        mode: str = "ins",
    ) -> pd.Series:
        dbg.dassert_isinstance(srs, pd.Series)
        if mode == "ins":
            srs = srs[: oos_start].copy()
        elif mode == "oos":
            srs = srs[oos_start :].copy()
        elif mode == "all_available":
            srs = srs.copy()
        else:
            raise ValueError(f"Unrecognized mode `{mode}`")
        if stats_name == "summarize_time_index_info":
            stats_val = cstati.summarize_time_index_info(srs)
            return stats_val
        
    @staticmethod
    def _calculate_model_stats():
        pass