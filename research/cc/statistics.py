"""
Compute crypto-related statistics.

Import as:

import research.cc.statistics as rccstat
"""
import logging
from typing import Callable, Union

import pandas as pd

import core.config.config_ as cconconf
import core.statistics as costatis
import helpers.dbg as hdbg
import helpers.hpandas as hpandas
import im.ccxt.data.load.loader as imcdalolo
import im.cryptodatadownload.data.load.loader as icdalolo
import im_v2.data.universe as imv2dauni

_LOG = logging.getLogger(__name__)


def compute_start_end_table(
    price_data: pd.DataFrame,
    config: cconconf.Config,
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
    hpandas.dassert_monotonic_index(price_data.index)
    hdbg.dassert_eq(price_data.index.freq, "T")
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


# TODO(Grisha): move `get_loader_for_vendor` out in #269.
# TODO(Grisha): use the abstract class in #313.
def get_loader_for_vendor(
    vendor: str, config: ccocon.Config
) -> Union[imccdaloloa.CcxtLoader, imcrdaloloa.CddLoader]:
    """
    Get vendor specific loader instance.

    :param config: config
    :param vendor: data provider, e.g. `CCXT`
    :return: loader instance
    """
    if vendor == "CCXT":
        loader = imcdalolo.CcxtLoader(
            root_dir=config["load"]["data_dir"],
            aws_profile=config["load"]["aws_profile"],
        )
    elif vendor == "CDD":
        loader = icdalolo.CddLoader(
            root_dir=config["load"]["data_dir"],
            aws_profile=config["load"]["aws_profile"],
        )
    else:
        raise ValueError(f"Unsupported vendor={vendor}")
    return loader


def compute_stats_for_universe(
    config: ccocon.Config,
    stats_func: Callable,
) -> pd.DataFrame:
    """
    Compute stats on the universe level.

    E.g., to compute start-end table for the universe do:
    `compute_stats_for_universe(config, compute_start_end_table, config)`.

    :param stats_func: function to compute statistics, e.g. `compute_start_end_table`
    :return: stats table for all vendors, exchanges, currencies in the universe
    """
    hdbg.dassert_isinstance(stats_func, Callable)
    universe = imdauni.get_trade_universe(config["data"]["universe_version"])
    stats_data = []
    for vendor in universe.keys():
        # Get vendor-specific loader.
        loader = get_loader_for_vendor(vendor, config)
        # Get vendor-specific universe.
        vendor_universe = universe[vendor]
        # Convert to a list of tuples `(exchange, currency_pair)` to avoid another `for loop`.
        exchange_currency_tuples = [
            (exchange, currency_pair)
            for exchange, currency_pairs in vendor_universe.items()
            for currency_pair in currency_pairs
        ]
        for exchange, currency_pair in exchange_currency_tuples:
            # Read data for current vendor, exchange, currency pair.
            data = loader.read_data_from_filesystem(
                exchange,
                currency_pair,
                config["data"]["data_type"],
            )
            # Compute stats on the exchange-currency level.
            cur_stats_data = stats_func(data)
            cur_stats_data["vendor"] = vendor
            stats_data.append(cur_stats_data)
    # Concatenate the results.
    stats_table = pd.concat(stats_data, ignore_index=True)
    return stats_table
