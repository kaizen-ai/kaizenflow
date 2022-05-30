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


def compare_data_stats(
        vendor1_df: pd.DataFrame,
        vendor2_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compare data statistics from two different vendors, i.e. bad data or timestamp statistics.

    :param vendor1_df: data statistics of some vendor, e.g. bad data of `CCXT`
    :param vendor2_df: data statistics of another vendor
    return: comparison of statistics of two vendors
    """
    vendor1_columns = vendor1_df.columns.to_list().sort()
    vendor2_columns = vendor2_df.columns.to_list().sort()
    # Check if columns are equal in order for it is used in `MultiIndex`.
    hdbg.dassert_eq(vendor1_columns, vendor2_columns)
    vendor_names = [vendor1_df.name, vendor2_columns.name]
    stats_comparison = pd.concat(
        [
            vendor1_df,
            vendor2_df,
        ],
        keys=vendor_names,
        axis=1,
    )
    # Drop stats for not intersecting time periods.
    stats_comparison = stats_comparison.dropna()
    # Reorder columns.
    stats_comparison.columns = stats_comparison.columns.swaplevel(0, 1)
    new_cols = stats_comparison.columns.reindex(vendor1_columns, level=0)
    stats_comparison = stats_comparison.reindex(columns=new_cols[0])
    return stats_comparison


def _preprocess_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess OHLCV data for QA stats computations.

    Preprocessing includes:
       - Replace NaNs with `np.inf` to differentiate them with missing bars
         after resampling
       - Resample data to count missing bars
       - Add year and month as columns to group by them while computing QA stats
    """
    # from missing bars.
    preprocessed_data = data.fillna(np.inf)
    # Resample data for each full symbol to insert missing bars. Data is
    # resampled for each full symbol because index must be unique to
    # perform resampling.
    resampled_symbol_data = []
    for _, symbol_data in preprocessed_data.groupby("full_symbol"):
        symbol_data = hpandas.resample_df(symbol_data, "T")
        hpandas.dassert_strictly_increasing_index(symbol_data)
        symbol_data["full_symbol"] = symbol_data["full_symbol"].fillna(
            method="bfill"
        )
        resampled_symbol_data.append(symbol_data)
    preprocessed_data = pd.concat(resampled_symbol_data)
    #
    preprocessed_data["year"] = preprocessed_data.index.year
    preprocessed_data["month"] = preprocessed_data.index.month
    return preprocessed_data


def get_bad_data_stats(data: pd.DataFrame, agg_level: List[str], vendor_name: str) -> pd.DataFrame:
    """
    Get QA stats per specified groups.

    QA stats include:
       - `bad data [%]` - sum of the metrics below
       - `missing bars [%]` - number of missing bars as %
       - `volume=0 [%]` - number of rows with volume = 0 as %
       - `NaNs [%]` - number of rows with `close` = NaN as %

    E.g,:
    ```
                                bad data [%]  ...  NaNs [%]
      full_symbol  year  month
    ftx::ADA_USDT  2021     11      3.5      0.0      6.0
                            12      2.4      0.0      5.1
    ftx::BTC_USDT  2022      1      1.5      0.0      0.0
    ```

    :param agg_level: columns to group data by
    """
    hdbg.dassert_lte(1, len(agg_level))
    # Copy in order not to modify original data.
    data_copy = data.copy()
    # Modify data for computing stats.
    data_copy = _preprocess_data(data_copy)
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
    res_stats_df.name = vendor_name
    return res_stats_df


def get_timestamp_stats(data: pd.DataFrame, vendor_name: str) -> pd.DataFrame:
    """
    Get timestamp stats per full symbol.

    Timestamps stats include:
       - Minimum timestamp
       - Maximum timestamp
       - Days available - difference between max and min timestamps in days

    E.g,:
    ```
                   min_timestamp    max_timestamp   days_available
    ftx::ADA_USDT  2021-08-07       2022-05-18      284
    ftx::BTC_USDT  2018-01-01       2022-05-18      1598
    ```
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
    res_stats_df.name = vendor_name
    return res_stats_df
