"""
Import as:

import core.stats_computer as cstats
"""

import collections
import logging
from typing import Callable, Dict, Iterable, List, Optional, Union

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
    def series_stats_methods(self) -> List[str]:
        methods = self._map_name_to_method("series").keys()
        return list(methods) + ["calculate_series_stats"]

    @property
    def model_stats_methods(self) -> List[str]:
        methods = self._map_name_to_method("model").keys()
        return list(methods) + ["calculate_model_stats"]

    # #########################################################################
    # Series stats methods
    # #########################################################################

    def summarize_time_index_info(
        self,
        srs: pd.Series,
    ) -> pd.Series:
        return self._calculate_series_stats(
            srs=srs,
            stats_names=["summarize_time_index_info"],
        )

    def compute_jensen_ratio(
        self,
        srs: pd.Series,
    ) -> pd.Series:
        return self._calculate_series_stats(
            srs=srs,
            stats_names=["compute_jensen_ratio"],
        )

    def compute_forecastability(
        self,
        srs: pd.Series,
    ) -> pd.Series:
        return self._calculate_series_stats(
            srs=srs,
            stats_names=["compute_forecastability"],
        )

    def compute_moments(
        self,
        srs: pd.Series,
    ) -> pd.Series:
        return self._calculate_series_stats(
            srs=srs,
            stats_names=["compute_moments"],
        )

    def compute_special_value_stats(
        self,
        srs: pd.Series,
    ) -> pd.Series:
        return self._calculate_series_stats(
            srs=srs,
            stats_names=["compute_special_value_stats"],
        )

    def apply_normality_test(
        self,
        srs: pd.Series,
    ) -> pd.Series:
        return self._calculate_series_stats(
            srs=srs,
            stats_names=["apply_normality_test"],
        )

    def apply_stationarity_tests(
        self,
        srs: pd.Series,
    ) -> pd.Series:
        return self._calculate_series_stats(
            srs=srs,
            stats_names=["apply_stationarity_tests"],
        )

    def calculate_series_stats(
        self,
        srs: pd.Series,
    ) -> pd.Series:
        """
        Calculate all available stats for a series as in
        `dataframe_modeler.calculate_stats`.
        """
        return self._calculate_series_stats(srs=srs)

    # #########################################################################
    # Model stats methods
    # #########################################################################

    def summarize_sharpe_ratio(
        self,
        srs: pd.Series,
    ) -> pd.Series:
        return self._calculate_model_stats(
            pnl=srs,
            stats_names=["summarize_sharpe_ratio"],
        )

    def ttest_1samp(
        self,
        srs: pd.Series,
    ) -> pd.Series:
        return self._calculate_model_stats(
            pnl=srs,
            stats_names=["ttest_1samp"],
        )

    def compute_kratio(
        self,
        srs: pd.Series,
    ) -> pd.Series:
        return self._calculate_model_stats(
            pnl=srs,
            stats_names=["compute_kratio"],
        )

    def compute_annualized_return_and_volatility(
        self,
        srs: pd.Series,
    ) -> pd.Series:
        return self._calculate_model_stats(
            pnl=srs,
            stats_names=["compute_annualized_return_and_volatility"],
        )

    def compute_max_drawdown(
        self,
        srs: pd.Series,
    ) -> pd.Series:
        return self._calculate_model_stats(
            pnl=srs,
            stats_names=["compute_max_drawdown"],
        )

    def calculate_hit_rate(
        self,
        srs: pd.Series,
    ) -> pd.Series:
        return self._calculate_model_stats(
            pnl=srs,
            stats_names=["calculate_hit_rate"],
        )

    def calculate_corr_to_underlying(
        self,
        pnl: pd.Series,
        returns: pd.Series,
    ) -> pd.Series:
        return self._calculate_model_stats(
            pnl=pnl,
            returns=returns,
            stats_names=["calculate_corr_to_underlying"],
        )

    def compute_bet_stats(
        self,
        positions: pd.Series,
        returns: pd.Series,
    ) -> pd.Series:
        return self._calculate_model_stats(
            positions=positions,
            returns=returns,
            stats_names=["compute_bet_stats"],
        )

    def compute_avg_turnover_and_holding_period(
        self,
        srs: pd.Series,
    ) -> pd.Series:
        return self._calculate_model_stats(
            positions=srs,
            stats_names=["compute_avg_turnover_and_holding_period"],
        )

    def compute_prediction_corr(
        self,
        returns: pd.Series,
        positions: pd.Series,
    ) -> pd.Series:
        return self._calculate_model_stats(
            returns=returns,
            positions=positions,
            stats_names=["compute_prediction_corr"],
        )

    def calculate_model_stats(
        self,
        returns: Optional[pd.Series] = None,
        positions: Optional[pd.Series] = None,
        pnl: Optional[pd.Series] = None,
    ) -> pd.Series:
        """
        Calculate all available stats for a model as in
        `model_evaluator.calculate_stats`.
        """
        return self._calculate_model_stats(
            returns=returns,
            positions=positions,
            pnl=pnl,
        )

    # #########################################################################
    # Helpers
    # #########################################################################

    @staticmethod
    def _apply_stationarity_tests(srs: pd.Series) -> pd.Series:
        return pd.concat(
            [
                cstati.apply_adf_test(srs, prefix="adf_"),
                cstati.apply_kpss_test(srs, prefix="kpss_"),
            ]
        )

    def _map_name_to_method(self, stats_type: str) -> Dict[str, Callable]:
        """
        Map `stats_names` to corresponding methods.

        :param stats_type: "series" or "model"
        :return: dict with stats names as keys and corresponding methods as values
        """
        if stats_type == "series":
            stats_names_dict = collections.OrderedDict(
                {
                    "summarize_time_index_info": cstati.summarize_time_index_info,
                    "compute_jensen_ratio": cstati.compute_jensen_ratio,
                    "compute_forecastability": cstati.compute_forecastability,
                    "compute_moments": cstati.compute_moments,
                    "compute_special_value_stats": cstati.compute_special_value_stats,
                    "apply_normality_test": lambda x: cstati.apply_normality_test(
                        x, prefix="normality_"
                    ),
                    "apply_stationarity_tests": self._apply_stationarity_tests,
                }
            )
        if stats_type == "model":
            stats_names_dict = collections.OrderedDict(
                {
                    "summarize_sharpe_ratio": cstati.summarize_sharpe_ratio,
                    "ttest_1samp": cstati.ttest_1samp,
                    "compute_kratio": lambda x: pd.Series(
                        cfinan.compute_kratio(x), index=["kratio"]
                    ),
                    "compute_annualized_return_and_volatility": (
                        cstati.compute_annualized_return_and_volatility
                    ),
                    "compute_max_drawdown": cstati.compute_max_drawdown,
                    "summarize_time_index_info": cstati.summarize_time_index_info,
                    "calculate_hit_rate": cstati.calculate_hit_rate,
                    "calculate_corr_to_underlying": lambda x: pd.Series(
                        x[0].corr(x[1]), index=["corr_to_underlying"]
                    ),
                    "compute_bet_stats": lambda x: cstati.compute_bet_stats(
                        x[0], x[1][x[0].index]
                    ),
                    "compute_avg_turnover_and_holding_period": (
                        cstati.compute_avg_turnover_and_holding_period
                    ),
                    "compute_jensen_ratio": cstati.compute_jensen_ratio,
                    "compute_forecastability": cstati.compute_forecastability,
                    "compute_prediction_corr": lambda x: pd.Series(
                        x[0].corr(x[1]), index=["prediction_corr"]
                    ),
                    "compute_moments": cstati.compute_moments,
                    "compute_special_value_stats": cstati.compute_special_value_stats,
                }
            )
        return stats_names_dict

    @staticmethod
    def _validate_stats_names(
        stats_names_available: Iterable[str],
        stats_names_requested: Optional[List[str]] = None,
    ) -> pd.Series:
        """
        Validate `stats_names` and sort according to the whole stats table
        logic.
        """
        stats_names = stats_names_requested or stats_names_available
        stats_names_diff = set(stats_names) - set(stats_names_available)
        if stats_names_diff:
            raise ValueError(f"Unsupported stats names: {stats_names_diff}")
        stats_names = [
            name for name in stats_names_available if name in stats_names
        ]
        return stats_names

    def _calculate_series_stats(
        self,
        srs: pd.Series,
        stats_names: Optional[List[str]] = None,
    ) -> pd.Series:
        dbg.dassert_isinstance(srs, pd.Series)
        stats_names_dict = self._map_name_to_method("series")
        stats_names = self._validate_stats_names(
            stats_names_dict.keys(), stats_names
        )
        stats_vals = []
        for stat_name in stats_names:
            stats_vals.append(stats_names_dict[stat_name](srs))
        return pd.concat(stats_vals)

    def _calculate_model_stats(
        self,
        returns: Optional[pd.Series] = None,
        positions: Optional[pd.Series] = None,
        pnl: Optional[pd.Series] = None,
        stats_names: Optional[List[str]] = None,
    ) -> pd.Series:
        inputs = [returns, positions, pnl]
        dbg.dassert(
            not pd.isna(inputs[1:]).all(), "At least pnl or positions should be not `None`."
        )
        freqs = {srs.index.freq for srs in inputs if srs is not None}
        dbg.dassert_eq(len(freqs), 1, "Series have different frequencies.")
        stats_names_dict = self._map_name_to_method("model")
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
                            stats_names_dict[stat_name]([srs, returns])
                        )
                else:
                    stats_vals.append(stats_names_dict[stat_name](srs))
        return pd.concat(stats_vals)
