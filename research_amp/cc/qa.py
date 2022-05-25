"""
Import as:

import research_amp.cc.qa as ramccqa
"""

import pandas as pd

import core.config.config_ as cconconf
import core.statistics as costatis


def get_bad_data_stats(
    config: cconconf.Config, data: pd.DataFrame, agg_level: str
) -> pd.DataFrame:
    """
    Get quality assurance stats per full symbol.
    """
    if agg_level == "by_year_month":
        agg_by = [config["column_names"]["full_symbol"], "year", "month"]
        data["year"] = data.index.year
        data["month"] = data.index.month
        aggregated_data = list(data.groupby(["year", "month"]))
        data_by_year_month = []
        for index in range(len(aggregated_data)):
            df = pd.DataFrame(aggregated_data[index][1])
            data_by_year_month.append(df)
        data = pd.concat(data_by_year_month)
    else:
        agg_by = config["column_names"]["full_symbol"]
    res_stats = []
    for full_symbol, symbol_data in data.groupby(agg_by):
        # Compute stats for a full symbol.
        symbol_stats = pd.Series(dtype="object", name=full_symbol)
        symbol_stats["NaNs [%]"] = 100 * (
            costatis.compute_frac_nan(
                symbol_data[config["column_names"]["close_price"]]
            )
        )
        symbol_stats["volume=0 [%]"] = 100 * (
            symbol_data[symbol_data["volume"] == 0].shape[0]
            / symbol_data.shape[0]
        )
        symbol_stats["bad data [%]"] = (
            symbol_stats["NaNs [%]"] + symbol_stats["volume=0 [%]"]
        )
        if agg_level == "by_year_month":
            year = symbol_data["year"].tolist()
            month = symbol_data["month"].tolist()
            symbol_stats["year"] = year
            symbol_stats["month"] = month
        res_stats.append(symbol_stats)
    # Combine all full symbol stats.
    res_stats_df = pd.concat(res_stats, axis=1).T
    if agg_level == "by_year_month":
        res_stats_df[config["column_names"]["full_symbol"]] = res_stats_df.index
        index_columns = [config["column_names"]["full_symbol"], "year", "month"]
        res_stats_df = res_stats_df.sort_values(index_columns)
        res_stats_df = res_stats_df.set_index(index_columns)
    return res_stats_df


def get_timestamp_stats(
    config: cconconf.Config, data: pd.DataFrame
) -> pd.DataFrame:
    """
    Get min max timestamp stats per full symbol.
    """
    res_stats = []
    for full_symbol, symbol_data in data.groupby(
        config["column_names"]["full_symbol"]
    ):
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
