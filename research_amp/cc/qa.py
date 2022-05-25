"""
Import as:

import research_amp.cc.qa as ramccqa
"""
from typing import List

import pandas as pd

import core.statistics as costatis


def get_bad_data_stats(data: pd.DataFrame, agg_level: List[str]) -> pd.DataFrame:
    """
    Get quality assurance stats per required columns.

    :param agg_level: list of columns to group data by
    """
    res_stats = []
    for full_symbol, symbol_data in data.groupby(agg_level):
        # Compute stats for a full symbol.
        symbol_stats = pd.Series(dtype="object", name=full_symbol)
        symbol_stats["NaNs [%]"] = 100 * (
            costatis.compute_frac_nan(symbol_data["close"])
        )
        symbol_stats["volume=0 [%]"] = 100 * (
            symbol_data[symbol_data["volume"] == 0].shape[0]
            / symbol_data.shape[0]
        )
        symbol_stats["bad data [%]"] = (
            symbol_stats["NaNs [%]"] + symbol_stats["volume=0 [%]"]
        )
        res_stats.append(symbol_stats)
    res_stats_df = pd.concat(res_stats, axis=1).T
    return res_stats_df


def get_timestamp_stats(data: pd.DataFrame) -> pd.DataFrame:
    """
    Get min max timestamp stats per full symbol.
    """
    res_stats = []
    for full_symbol, symbol_data in data.groupby("full_symbol"):
        # Compute stats for a full symbol.
        symbol_stats = pd.Series(dtype="object", name=full_symbol)
        index = symbol_data.index
        symbol_stats["min_timestamp"] = index.min()
        symbol_stats["max_timestamp"] = index.max()
        symbol_stats["days_available"] = (
            symbol_stats["max_timestamp"] - symbol_stats["min_timestamp"]
        ).days
        res_stats.append(symbol_stats)
    # Combine all full symbol stats.
    res_stats_df = pd.concat(res_stats, axis=1).T
    return res_stats_df
