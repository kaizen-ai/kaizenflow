from typing import List, Union

import pandas as pd

import core.statistics as cstati
import helpers.dbg as hdbg
import helpers.hpandas as hpandas


def compute_start_end_table(
    price_data: pd.DataFrame,
    group_by_cols: Union[str, List[str]],
    close_price_col: str,
) -> pd.DataFrame:
    """
    Compute start-end table on exchange-currency level.

    Start-end table's structure is:
        - exchange name
        - currency pair
        - minimum observed timestamp
        - maximum observed timestamp
        - the number of data points
        - the number of days for which data is available
        - average number of data points per day
        - data coverage, which is actual number of observations divided by
          expected number of observations (assuming 1 minute resolution)
          as percentage

    :param price_data: crypto price data
    :param group_by_cols:
    :param close_price_col:
    :return: start-end table
    """
    # hdbg.dassert_is_subset(group_by_cols, price_data.columns)
    hdbg.dassert_in(close_price_col, price_data.columns)
    # ....
    hdbg.dassert_isinstance(price_data.index, pd.DatetimeIndex)
    hpandas.dassert_monotonic_index(price_data.index)
    # ....
    price_data[group_by_cols] = price_data[group_by_cols].fillna(method="ffill")
    # Group by........
    price_data_grouped = price_data.groupby(group_by_cols, dropna=False)
    # Compute the stats.
    start_end_table = (
        price_data_grouped[close_price_col]
        .agg(
            min_timestamp="idxmin",
            max_timestamp="idxmax",
            n_data_points="count",
            coverage=lambda x: round((1 - cstati.compute_frac_nan(x)) * 100, 2),
        )
        .reset_index()
    )
    start_end_table["days_available"] = (
        start_end_table["max_timestamp"] - start_end_table["min_timestamp"]
    ).dt.days
    start_end_table["avg_data_points_per_day"] = (
        start_end_table["n_data_points"] / start_end_table["days_available"]
    )
    return start_end_table
