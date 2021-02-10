"""
Import as:

import core.stats_computer as cstats
"""

import logging
from typing import List, Optional, Union

import pandas as pd

import core.statistics as cstati
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class StatsComputer:
    """
    Allows to get particular piece of stats instead of the whole stats table.

    Available methods for series stats:
        - summarize_time_index_info
        - compute_jensen_ratio
        - compute_forecastability
        - compute_moments
        - compute_special_value_stats
        - apply_normality_test
        - apply_stationarity_test
    Available methods for model stats:
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

    def apply_normality_test(
        self,
        srs: pd.Series,
        oos_start: Optional[Union[str, pd.Timestamp]] = None,
        mode: str = "ins",
    ) -> pd.Series:
        return self._calculate_series_stats(
            srs=srs,
            stats_names=["apply_normality_test"],
            oos_start=oos_start,
            mode=mode,
        )

    def apply_stationarity_test(
        self,
        srs: pd.Series,
        oos_start: Optional[Union[str, pd.Timestamp]] = None,
        mode: str = "ins",
    ) -> pd.Series:
        return self._calculate_series_stats(
            srs=srs,
            stats_names=["apply_stationarity_test"],
            oos_start=oos_start,
            mode=mode,
        )

    def calculate_series_stats(
        self,
        srs: pd.Series,
        oos_start: Optional[Union[str, pd.Timestamp]] = None,
        mode: str = "ins",
    ) -> pd.Series:
        return self._calculate_series_stats(
            srs=srs,
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
            return srs[:oos_start].copy()
        if mode == "oos":
            return srs[oos_start:].copy()
        if mode == "all_available":
            return srs.copy()
        raise ValueError(f"Unrecognized mode `{mode}`")

    @staticmethod
    def _apply_stationarity_test(srs: pd.Series) -> pd.Series:
        return pd.concat(
            [
                cstati.apply_adf_test(srs, prefix="adf_"),
                cstati.apply_kpss_test(srs, prefix="kpss_"),
            ]
        )

    def _calculate_series_stats(
        self,
        srs: pd.Series,
        stats_names: Optional[List[str]] = None,
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
            "apply_normality_test": lambda x: cstati.apply_normality_test(
                x, prefix="normality_"
            ),
            "apply_stationarity_test": self._apply_stationarity_test,
        }
        # Check `stats_names` and sort according to the whole stats table logic.
        stats_names = stats_names or stats_names_dict.keys()
        stats_names_diff = set(stats_names) - set(stats_names_dict.keys())
        if stats_names_diff:
            raise ValueError(f"Unsupported stats names: {stats_names_diff}")
        stats_names = [name for name in stats_names_dict if name in stats_names]
        # Calculate and combine stats.
        stats_vals = []
        for stat_name in stats_names:
            stats_vals.append(stats_names_dict[stat_name](srs))
        return pd.concat(stats_vals)

    @staticmethod
    def _calculate_model_stats():
        pass
