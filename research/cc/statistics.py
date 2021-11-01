import pandas as pd

import core.config.config_ as ccocon
import core.statistics as cstati
import helpers.dbg as hdbg
import helpers.hpandas as hpandas


def compute_start_end_table(
    price_data: pd.DataFrame,
    config: ccocon.Config,
) -> pd.DataFrame:
    """
    Compute start-end table on exchange-currency level.

    Note: `price_data` must be resampled using NaNs.

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
    :return: start-end table
    """
    hdbg.dassert_is_subset(
        [
            config["column_names"]["close_price"],
            config["column_names"]["currency_pair"],
            config["column_names"]["exchange"],
        ],
        price_data.columns
    )
    #
    hdbg.dassert_isinstance(price_data.index, pd.DatetimeIndex)
    hpandas.dassert_monotonic_index(price_data.index)
    #
    group_by_columns = [config["column_names"]["exchange"], config["column_names"]["currency_pair"]]
    # For NaN close prices all the columns are NaN due to resampling, since the
    # analysis is on exchange-currency level, the NaNs are filled with existing
    # exchange name and currency pair.
    price_data[group_by_columns] = price_data[group_by_columns].fillna(method="ffill")
    price_data_grouped = price_data.groupby(group_by_columns, dropna=False)
    # Compute the stats.
    start_end_table = (
        price_data_grouped[config["column_names"]["close_price"]]
        .agg(
            min_timestamp="idxmin",
            max_timestamp="idxmax",
            # Count only not NaN observations.
            n_data_points="count",
            coverage=lambda x: round((1 - cstati.compute_frac_nan(x)) * 100, 2),
        )
        .reset_index()
    )
    start_end_table["days_available"] = (
        start_end_table["max_timestamp"] - start_end_table["min_timestamp"]
    ).dt.days
    start_end_table["avg_data_points_per_day"] = round((
        start_end_table["n_data_points"] / start_end_table["days_available"]
    ), 2)
    return start_end_table
