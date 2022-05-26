"""
Import as:

import research_amp.cc.qa as ramccqa
"""
from typing import List

import numpy as np
import pandas as pd

import core.statistics as costatis
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas


def preprocess_data_for_qa_stats_computation(data: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess vendor data for QA stats computations.
    """
    # Fill NaN values with `np.inf` in order to differentiate them
    # from missing bars.
    preprocessed_data = data.fillna(np.inf)
    # Resample data for each full symbol to insert missing bars.
    resampled_symbol_data = []
    for full_symbol, symbol_data in preprocessed_data.groupby("full_symbol"):
        symbol_data = hpandas.resample_df(symbol_data, "T")
        symbol_data["full_symbol"] = symbol_data["full_symbol"].fillna(
            method="bfill"
        )
        resampled_symbol_data.append(symbol_data)
    preprocessed_data = pd.concat(resampled_symbol_data)
    # Add year and month columns to allow grouping data by them.
    preprocessed_data["year"] = preprocessed_data.index.year
    preprocessed_data["month"] = preprocessed_data.index.month
    return preprocessed_data


def get_bad_data_stats(data: pd.DataFrame, agg_level: List[str]) -> pd.DataFrame:
    """
    Get quality assurance stats per required columns.

    :param agg_level: list of columns to group data by
    """
    hdbg.dassert_lte(1, len(agg_level))
    # Copy in order not to modify original data.
    data_copy = data.copy()
    # Modify data for computing stats.
    data_copy = preprocess_data_for_qa_stats_computation(data_copy)
    # Check that columns to group by exist.
    hdbg.dassert_is_subset(agg_level, data_copy.columns)
    res_stats = []
    for full_symbol, symbol_data in data_copy.groupby(agg_level):
        # Compute stats for a full symbol.
        symbol_stats = pd.Series(dtype="object", name=full_symbol)
        # Compute NaNs in initially loaded data by counting `np.inf` values
        # in preprocessed data.
        symbol_stats["NaNs [%]"] = 100 * (
            symbol_data[symbol_data["close"] == np.inf].shape[0]
            / symbol_data.shape[0]
        )
        # Compute missing bars stats by subtracting NaN stats in not-resampled
        # data from NaN stats in resampled data.
        symbol_stats["missing bars [%]"] = 100 * (
            costatis.compute_frac_nan(symbol_data["close"])
        )
        symbol_stats["volume=0 [%]"] = 100 * (
            symbol_data[symbol_data["volume"] == 0].shape[0]
            / symbol_data.shape[0]
        )
        symbol_stats["bad data [%]"] = (
            symbol_stats["NaNs [%]"]
            + symbol_stats["missing bars [%]"]
            + symbol_stats["volume=0 [%]"]
        )
        res_stats.append(symbol_stats)
    res_stats_df = pd.concat(res_stats, axis=1).T
    cols = ["bad data [%]", "missing bars [%]", "volume=0 [%]", "NaNs [%]"]
    res_stats_df = res_stats_df[cols]
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
