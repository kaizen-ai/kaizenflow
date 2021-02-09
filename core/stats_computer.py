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
        - compute_jensen_ratio
        - compute_forecastability
        - compute_moments
        - compute_special_value_stats
        - apply_normality_tests
    """
    def summarize_time_index_info(
        self, 
        srs: pd.Series,
        oos_start: Optional[Union[str, pd.Timestamp]] = None,
        mode: str = "ins",
    ) -> pd.Series:
        return self._calculate_series_stats(
            srs=srs,
            stats_names=["summarize_time_index_info"],
            oos_start=oos_start,
            mode=mode,
        )
    
    def compute_jensen_ratio(
        self, 
        srs: pd.Series,
        oos_start: Optional[Union[str, pd.Timestamp]] = None,
        mode: str = "ins",
    ) -> pd.Series:
        return self._calculate_series_stats(
            srs=srs,
            stats_names=["compute_jensen_ratio"],
            oos_start=oos_start,
            mode=mode,
        )
    
    def compute_forecastability(
        self, 
        srs: pd.Series,
        oos_start: Optional[Union[str, pd.Timestamp]] = None,
        mode: str = "ins",
    ) -> pd.Series:
        return self._calculate_series_stats(
            srs=srs,
            stats_names=["compute_forecastability"],
            oos_start=oos_start,
            mode=mode,
        )
    
    def compute_moments(
        self, 
        srs: pd.Series,
        oos_start: Optional[Union[str, pd.Timestamp]] = None,
        mode: str = "ins",
    ) -> pd.Series:
        return self._calculate_series_stats(
            srs=srs,
            stats_names=["compute_moments"],
            oos_start=oos_start,
            mode=mode,
        )
    
    def compute_special_value_stats(
        self, 
        srs: pd.Series,
        oos_start: Optional[Union[str, pd.Timestamp]] = None,
        mode: str = "ins",
    ) -> pd.Series:
        return self._calculate_series_stats(
            srs=srs,
            stats_names=["compute_special_value_stats"],
            oos_start=oos_start,
            mode=mode,
        )
    
    def apply_normality_tests(
        self, 
        srs: pd.Series,
        oos_start: Optional[Union[str, pd.Timestamp]] = None,
        mode: str = "ins",
    ) -> pd.Series:
        return self._calculate_series_stats(
            srs=srs,
            stats_names=["apply_normality_tests"],
            oos_start=oos_start,
            mode=mode,
        )
    
    @staticmethod
    def _get_srs(
        srs: pd.Series,
        oos_start: Optional[Union[str, pd.Timestamp]] = None,
        mode: str = "ins",
    ) -> pd.Series:
        if mode == "ins":
            return srs[: oos_start].copy()
        if mode == "oos":
            return srs[oos_start :].copy()
        if mode == "all_available":
            return srs.copy()
        raise ValueError(f"Unrecognized mode `{mode}`")
    
    @staticmethod
    def _apply_normality_tests(srs: pd.Series) -> pd.Series:
        return pd.concat(
            [cstati.apply_normality_test(srs, prefix="normality_"),
             cstati.apply_adf_test(srs, prefix="adf_"),
             cstati.apply_kpss_test(srs, prefix="kpss_"),]
        )
        
    def _calculate_series_stats(
        self,
        srs: pd.Series, 
        stats_names: List[str],
        oos_start: Optional[Union[str, pd.Timestamp]] = None,
        mode: str = "ins",
    ) -> pd.Series:
        dbg.dassert_isinstance(srs, pd.Series)
        srs = self._get_srs(srs, mode=mode, oos_start=oos_start)
        # Map `stats_names` to corresponding functions.
        stats_names_dict = {
            "summarize_time_index_info": cstati.summarize_time_index_info,
            "compute_jensen_ratio": cstati.compute_jensen_ratio,
            "compute_forecastability": cstati.compute_forecastability,
            "compute_moments": cstati.compute_moments,
            "compute_special_value_stats": cstati.compute_special_value_stats,
            "apply_normality_tests": self._apply_normality_tests, 
        }
        # Sort `stats_names` according to the whole stats table logic.
        stats_names = [name for name in stats_names_dict if name in stats_names]
        # Calculate and combine stats.
        stats_vals = []
        for stat_name in stats_names:
            stats_vals.append(stats_names_dict[stat_name](srs))
        return pd.concat(stats_vals)
        
    @staticmethod
    def _calculate_model_stats():
        pass