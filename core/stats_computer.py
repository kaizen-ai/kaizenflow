"""
Import as:

import core.stats_computer as cstats
"""

import collections
import logging
from typing import Callable, Dict, Iterable, List, Optional

import pandas as pd

import core.finance as cfinan
import core.statistics as cstati
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class StatsComputer:
    """
    Allows to get particular piece of stats instead of the whole stats table.
    """

    @property
    def get_stats_methods(self) -> List[str]:
        methods = self._map_name_to_method().keys()
        return list(methods) + ["calculate_stats"]

    @staticmethod
    def summarize_time_index_info(srs: pd.Series) -> pd.Series:
        return cstati.summarize_time_index_info(srs)

    @staticmethod
    def compute_jensen_ratio(srs: pd.Series) -> pd.Series:
        return cstati.compute_jensen_ratio(srs)

    @staticmethod
    def compute_forecastability(srs: pd.Series) -> pd.Series:
        return cstati.compute_forecastability(srs)

    @staticmethod
    def compute_moments(srs: pd.Series) -> pd.Series:
        return cstati.compute_moments(srs)

    @staticmethod
    def compute_special_value_stats(srs: pd.Series) -> pd.Series:
        return cstati.compute_special_value_stats(srs)

    def calculate_stats(self, *args, **kwargs) -> pd.Series:
        return self._calculate_stats(stats_names=None, *args, **kwargs)

    def _map_name_to_method(self) -> Dict[str, Callable]:
        """
        Map `stats_names` to corresponding methods.
        """
        stats_names_dict = collections.OrderedDict(
            {
                "summarize_time_index_info": self.summarize_time_index_info,
                "compute_jensen_ratio": self.compute_jensen_ratio,
                "compute_forecastability": self.compute_forecastability,
                "compute_moments": self.compute_moments,
                "compute_special_value_stats": self.compute_special_value_stats,
            }
        )
        return stats_names_dict

    @staticmethod
    def _validate_stats_names(
        stats_names_available: Iterable[str],
        stats_names_requested: Optional[List[str]] = None,
    ) -> pd.Series:
        stats_names = stats_names_requested or stats_names_available
        stats_names_diff = set(stats_names) - set(stats_names_available)
        if stats_names_diff:
            raise ValueError(f"Unsupported stats names: {stats_names_diff}")
        return stats_names

    def _calculate_stats(
        self,
        srs: pd.Series,
        stats_names: Optional[List[str]] = None,
    ) -> pd.Series:
        dbg.dassert_isinstance(srs, pd.Series)
        stats_names_dict = self._map_name_to_method()
        stats_names = self._validate_stats_names(
            stats_names_dict.keys(), stats_names
        )
        stats_vals = []
        for stat_name in stats_names:
            stats_vals.append(stats_names_dict[stat_name](srs))
        return pd.concat(stats_vals)


class SeriesStatsComputer(StatsComputer):
    """
    Class for series stats only.
    """

    @staticmethod
    def apply_normality_test(srs: pd.Series) -> pd.Series:
        return cstati.apply_normality_test(srs, prefix="normality_")

    @staticmethod
    def apply_stationarity_tests(srs: pd.Series) -> pd.Series:
        return pd.concat(
            [
                cstati.apply_adf_test(srs, prefix="adf_"),
                cstati.apply_kpss_test(srs, prefix="kpss_"),
            ]
        )

    def _map_name_to_method(self) -> Dict[str, Callable]:
        stats_names_dict = super()._map_name_to_method()
        stats_names_dict.update(
            collections.OrderedDict(
                {
                    "apply_normality_test": self.apply_normality_test,
                    "apply_stationarity_tests": self.apply_stationarity_tests,
                }
            )
        )
        return stats_names_dict


class ModelStatsComputer(StatsComputer):
    """
    Class for model stats only.
    """

    @staticmethod
    def summarize_sharpe_ratio(srs: pd.Series) -> pd.Series:
        return cstati.summarize_sharpe_ratio(srs)

    @staticmethod
    def ttest_1samp(srs: pd.Series) -> pd.Series:
        return cstati.ttest_1samp(srs)

    @staticmethod
    def compute_kratio(srs: pd.Series) -> pd.Series:
        return pd.Series(cfinan.compute_kratio(srs), index=["kratio"])

    @staticmethod
    def compute_annualized_return_and_volatility(srs: pd.Series) -> pd.Series:
        return cstati.compute_annualized_return_and_volatility(srs)

    @staticmethod
    def compute_max_drawdown(srs: pd.Series) -> pd.Series:
        return cstati.compute_max_drawdown(srs)

    @staticmethod
    def calculate_hit_rate(srs: pd.Series) -> pd.Series:
        return cstati.calculate_hit_rate(srs)

    @staticmethod
    def calculate_corr_to_underlying(
        srs1: pd.Series, srs2: pd.Series
    ) -> pd.Series:
        return pd.Series(srs1.corr(srs2), index=["corr_to_underlying"])

    @staticmethod
    def compute_bet_stats(srs1: pd.Series, srs2: pd.Series) -> pd.Series:
        return cstati.compute_bet_stats(srs1, srs2[srs1.index])

    @staticmethod
    def compute_avg_turnover_and_holding_period(srs: pd.Series) -> pd.Series:
        return cstati.compute_avg_turnover_and_holding_period(srs)

    @staticmethod
    def compute_prediction_corr(srs1: pd.Series, srs2: pd.Series) -> pd.Series:
        return pd.Series(srs1.corr(srs2), index=["prediction_corr"])

    def _map_name_to_method(self) -> Dict[str, Callable]:

        stats_names_dict = collections.OrderedDict(
            {
                "summarize_sharpe_ratio": self.summarize_sharpe_ratio,
                "ttest_1samp": self.ttest_1samp,
                "compute_kratio": self.compute_kratio,
                "compute_annualized_return_and_volatility": self.compute_annualized_return_and_volatility,
                "compute_max_drawdown": self.compute_max_drawdown,
                "calculate_hit_rate": self.calculate_hit_rate,
                "calculate_corr_to_underlying": self.calculate_corr_to_underlying,
                "compute_bet_stats": self.compute_bet_stats,
                "compute_avg_turnover_and_holding_period": self.compute_avg_turnover_and_holding_period,
                "compute_prediction_corr": self.compute_prediction_corr,
            }
        )
        stats_names_dict.update(super()._map_name_to_method())
        return stats_names_dict

    def _calculate_stats(
        self,
        pnl: Optional[pd.Series] = None,
        positions: Optional[pd.Series] = None,
        returns: Optional[pd.Series] = None,
        stats_names: Optional[List[str]] = None,
    ) -> pd.Series:
        dbg.dassert(
            not pd.isna([pnl, positions]).all(),
            "At least pnl or positions should be not `None`.",
        )
        freqs = {
            srs.index.freq for srs in [pnl, positions, returns] if srs is not None
        }
        dbg.dassert_eq(len(freqs), 1, "Series have different frequencies.")
        stats_names_dict = self._map_name_to_method()
        stats_names = self._validate_stats_names(
            stats_names_dict.keys(), stats_names
        )
        stats_vals = []
        positions_stats_names = [
            "compute_bet_stats",
            "compute_prediction_corr",
            "compute_avg_turnover_and_holding_period",
        ]
        returns_stats_names = [
            "compute_bet_stats",
            "compute_prediction_corr",
            "calculate_corr_to_underlying",
        ]
        for stat_name in stats_names:
            srs = positions if stat_name in positions_stats_names else pnl
            if srs is not None:
                if stat_name in returns_stats_names:
                    if returns is not None:
                        stats_vals.append(
                            stats_names_dict[stat_name](srs, returns)
                        )
                else:
                    stats_vals.append(stats_names_dict[stat_name](srs))
        return pd.concat(stats_vals)
