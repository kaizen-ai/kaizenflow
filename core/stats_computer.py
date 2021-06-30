"""
Import as:

import core.stats_computer as cstats
"""

import logging

import pandas as pd

import core.finance as cfinan
import core.statistics as cstati
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class StatsComputer:
    """
    Allows to get particular piece of stats instead of the whole stats table.
    """

    @staticmethod
    def compute_sampling_stats(srs: pd.Series) -> pd.Series:
        name = "sampling"
        #
        time_index_info = cstati.summarize_time_index_info(srs)
        time_index_info.name = name
        #
        special_value_stats = cstati.compute_special_value_stats(srs)
        special_value_stats.name = name
        return pd.concat([time_index_info, special_value_stats])

    @staticmethod
    def compute_summary_stats(srs: pd.Series) -> pd.Series:
        name = "summary"
        #
        moments = cstati.compute_moments(srs)
        moments.name = name
        #
        descriptive = srs.describe()
        descriptive.name = name
        #
        jensen = cstati.compute_jensen_ratio(srs)
        jensen.name = name
        #
        ttest = cstati.ttest_1samp(srs)
        ttest.name = name
        # TODO(*): Add
        #   - var and std assuming zero mean
        return pd.concat([moments, descriptive])

    @staticmethod
    def compute_stationarity_stats(srs: pd.Series) -> pd.Series:
        name = "stationarity"
        #
        adf = cstati.apply_adf_test(srs, prefix="adf_")
        adf.name = name
        #
        kpss = cstati.apply_kpss_test(srs, prefix="kpss_")
        kpss.name = name
        return pd.concat([adf, kpss])

    @staticmethod
    def compute_normality_stats(srs: pd.Series) -> pd.Series:
        name = "normality"
        #
        normality = cstati.apply_normality_test(srs)
        normality.name = name
        # TODO(*): cstati.compute_centered_gaussian_log_likelihood
        return pd.concat([normality])

    @staticmethod
    def compute_autocorrelation_stats(srs: pd.Series) -> pd.Series:
        # name = "autocorrelation"
        # ljung_box = cstati.apply_ljung_box_test(srs)
        # TODO(Paul): Only return pvals. Rename according to test and lag.
        #     Change default lags reported.
        raise NotImplementedError

    @staticmethod
    def compute_spectral_stats(srs: pd.Series) -> pd.Series:
        # TODO(Paul): `compute_forecastability()` goes here.
        raise NotImplementedError

    @staticmethod
    def compute_signal_quality_stats(srs: pd.Series) -> pd.Series:
        name = "signal_quality"
        #
        sharpe_ratio = cstati.summarize_sharpe_ratio(srs)
        sharpe_ratio.name = name
        #
        ttest = cstati.ttest_1samp(srs)
        ttest.name = name
        #
        kratio = pd.Series(cfinan.compute_kratio(srs), index=["kratio"])
        kratio.name = name
        #
        return pd.concat([sharpe_ratio, ttest, kratio])

    @staticmethod
    def compute_finance_stats(srs: pd.Series, mode: str) -> pd.Series:
        """
        Assumes `srs` is a PnL curve.

        :mode: pnl, positions
        """
        name = "finance"
        if mode == "pnl":
            ret_and_vol = cstati.compute_annualized_return_and_volatility(srs)
            ret_and_vol.name = name
            #
            drawdown = cstati.compute_max_drawdown(srs)
            drawdown.name = name
            #
            hit_rate = cstati.calculate_hit_rate(srs)
            hit_rate.name = name
            #
            return pd.concat([ret_and_vol, drawdown, hit_rate])
        elif mode == "positions":
            turnover = cstati.compute_avg_turnover_and_holding_period(srs)
            turnover.name = name
            return pd.concat([turnover])
        else:
            raise ValueError

    @staticmethod
    def calculate_bet_stats(positions: pd.Series, returns: pd.Series) -> pd.Series:
        name = "bets"
        bets = cstati.compute_bet_stats(positions, returns[positions.index])
        bets.name = name
        return bets

    # TODO(Paul): Add correlation for calculating
    #   - correlation of pnl to rets
    #   - correlation of predictions to rets
