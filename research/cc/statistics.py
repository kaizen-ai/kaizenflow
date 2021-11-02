"""
Compute crypto-related statistics.

Import as:

import research.cc.statistics as rccsta
"""
from typing import Any, Callable, Dict, List

import pandas as pd

import core.config.config_ as ccocon
import core.statistics as csta
import helpers.dbg as hdbg
import helpers.hpandas as hhpandas
import im.data.universe as imdauni


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
        - the number of not NaN data points
        - the number of days for which data is available
        - average number of not NaN data points per day
        - data coverage, which is the number of not NaN data points divided
          by the number of all data points as percentage

    :param price_data: crypto price data
    :return: start-end table
    """
    hdbg.dassert_is_subset(
        [
            config["column_names"]["close_price"],
            config["column_names"]["currency_pair"],
            config["column_names"]["exchange"],
        ],
        price_data.columns,
    )
    #
    hdbg.dassert_isinstance(price_data.index, pd.DatetimeIndex)
    hhpandas.dassert_monotonic_index(price_data.index)
    # Reset the index to use it for stats computation. The index's new column name
    # will be `index`.
    price_data_reset = price_data.reset_index()
    group_by_columns = [
        config["column_names"]["exchange"],
        config["column_names"]["currency_pair"],
    ]
    # For NaN close prices all the columns are NaN due to resampling, since the
    # analysis is on exchange-currency level, the NaNs are filled with existing
    # exchange name and currency pair.
    price_data_reset[group_by_columns] = price_data_reset[
        group_by_columns
    ].fillna(method="ffill")
    price_data_grouped = price_data_reset.groupby(
        group_by_columns, dropna=False, as_index=False
    )
    # Compute the stats.
    start_end_table = price_data_grouped.agg(
        min_timestamp=("index", "min"),
        max_timestamp=("index", "max"),
        n_data_points=(config["column_names"]["close_price"], "count"),
        coverage=(
            config["column_names"]["close_price"],
            lambda x: round((1 - csta.compute_frac_nan(x)) * 100, 2),
        ),
    )
    start_end_table["days_available"] = (
        start_end_table["max_timestamp"] - start_end_table["min_timestamp"]
    ).dt.days
    start_end_table["avg_data_points_per_day"] = round(
        (start_end_table["n_data_points"] / start_end_table["days_available"]), 2
    )
    return start_end_table


def get_loader_for_vendor(vendor: str, config: ccocon.Config) -> _LOADER:
    """
    Get vendor specific loader instance.

    :param vendor: data provider, e.g. `CCXT`
    :return: loader instance
    """
    if vendor == "CCXT":
        loader = imccdaloloa.CcxtLoader(
            root_dir=config["load"]["data_dir"],
            aws_profile=config["load"]["aws_profile"],
        )
    elif vendor == "CDD":
        loader = imcrdaloloa.CddLoader(
            root_dir=config["load"]["data_dir"],
            aws_profile=config["load"]["aws_profile"],
        )
    else:
        raise ValueError(f"Unsupported vendor={vendor}")
    return loader


def compute_stats(
    exchange,
    currency_pair,
    config,
    loader,
    stats_func: Callable,
    *args: Any,
    **kwargs: Any,
):
    # Read data for current data provider, exchange, currency pair.
    data = loader.read_data_from_filesystem(
        exchange,
        currency_pair,
        config["data"]["data_type"],
    )
    # Remove duplicates.
    # TODO(Grisha): move it into the loader.
    data_no_dups = data.drop_duplicates()
    # Resample data to target frequency using NaNs.
    data_resampled = hhpandas.resample_df(
        data_no_dups, config["data"]["target_frequency"]
    )
    # Compute stats.
    stats_table = stats_func(
        data_resampled, config, *args, **kwargs
    )
    return stats_table


def compute_stats_for_vendor(
    vendor_universe: Dict[str, List[str]],
    loader,
    config: ccocon.Config
) -> pd.DataFrame:
    """
    Same as `compute_start_end_table` but for all exchanges, currency pairs
    available for a certain vendor.

    :param vendor_universe: all exchanges, currency pairs avaiable for a vendor
    :param loader: vendor specific loader instance
    :return: vendor specific start-end table
    """
    stats_data = []
    for exchange in vendor_universe.keys():
        # Get the downloaded currency pairs for a particular exchange.
        currency_pairs = vendor_universe[exchange]
        for currency_pair in currency_pairs:

            stats_data.append(cur_start_end_table)
    # Concatenate the results.
    stats_table = pd.concat(stats_data, ignore_index=True)
    return stats_table


def compute_stats_for_universe(
    universe: imdauni.UNIVERSE,
    config,
    stats_func: Callable,
) -> pd.DataFrame:
    """
    Compute stats on the universe level.

    :return: stats table for all vendors in the universe
    """
    # Exclude CDD for now since there are many problems with data.
    # TODO(Grisha): file a bug about CDD data.
    universe.pop("CDD")
    # TODO(Grisha): fix the duplicates problem in #274.
    universe["CCXT"].pop("bitfinex")
    #
    stats_data = []
    for vendor in universe.keys():
        # Get vendor-specific universe.
        vendor_universe = universe[vendor]
        # Get vendor-specific loader.
        loader = get_loader_for_vendor(vendor, config)
        # Compute stats for the current vendor.
        cur_stats_data = compute_stats_for_vendor(
            vendor_universe, loader, config
        )
        cur_stats_data["vendor"] = vendor
        stats_data.append(cur_stats_data)
    # Concatenate the results.
    stats_table = pd.concat(stats_data, ignore_index=True)
    return stats_table
