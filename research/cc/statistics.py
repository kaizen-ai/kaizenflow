"""
Compute crypto-related statistics.

Import as:

import research.cc.statistics as rccsta
"""
import logging
from typing import Callable, Dict, List, Union

import numpy as np
import pandas as pd

import core.config.config_ as ccocon
import core.statistics as csta
import helpers.dbg as hdbg
import helpers.hpandas as hhpandas
import im.ccxt.data.load.loader as imccdaloloa
import im.cryptodatadownload.data.load.loader as imcrdaloloa
import im.data.universe as imdauni

_LOG = logging.getLogger(__name__)


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


def find_longest_not_nan_sequence(
    data: Union[pd.Series, pd.DataFrame]
) -> Union[pd.Series, pd.DataFrame]:
    """
    Find the longest sequence of not-NaN values in a series or dataframe.

    For a dataframe the longest sequence of rows with no NaN values is returned.

    :param data: input series or dataframe
    :return: longest sequence of not-NaN values
    """
    # Verify that index is monotonically increasing.
    hhpandas.dassert_strictly_increasing_index(data)
    # Get index frequency.
    freq = pd.infer_freq(data.index)
    # Get indices of only not-NaN values.
    not_nan_index = np.array(data.dropna().index)
    # Get a mask to distinguish not-NaN values that are further from their
    # not-NaN precedent than 1 frequency time step.
    mask = np.where(np.diff(not_nan_index) != pd.Timedelta(1, freq))[0] + 1
    # Get the longest monotonically increasing sequence of indices.
    longest_not_nan_index = max(np.split(not_nan_index, mask), key=len)
    # Get the longest sequence of not-NaN values.
    longest_not_nan_seq = data.loc[longest_not_nan_index].copy()
    return longest_not_nan_seq


def compute_longest_not_nan_sequence_stats(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Compute stats about the longest not-NaN sequence in each dataframe column.

    :param df: input dataframe
    :return: stats dataframe
    """
    # Initiate results dict.
    res_dict = {}
    # Iterate over each series in columns.
    for colname in df.columns:
        col_srs = df[colname].copy()
        # Remove leading and trailing NaNs.
        first_idx = col_srs.first_valid_index()
        last_idx = col_srs.last_valid_index()
        col_srs = col_srs[first_idx:last_idx].copy()
        # Get the longest not-NaN sequence in a series.
        longest_not_nan_seq = find_longest_not_nan_sequence(col_srs)
        # Compute necessary stats and put in a list.
        stats = [
            100 * (1 - csta.compute_frac_nan(col_srs)),
            (last_idx - first_idx).days,
            100 * (len(longest_not_nan_seq) / len(col_srs)),
            longest_not_nan_seq.index[0],
            longest_not_nan_seq.index[-1],
        ]
        # Append stats list to the result dict under a column name key.
        res_dict[colname] = stats
    # Build a dataframe from the result dict.
    res_df = pd.DataFrame.from_dict(
        res_dict,
        orient="index",
        columns=[
            "coverage",
            "longest_seq_days_available",
            "share_of_longest_seq",
            "longest_seq_start_date",
            "longest_seq_end_date",
        ],
    )
    # Sort by coverage and share of longest not-NaN sequence.
    res_df = res_df.sort_values(by=["coverage", "share_of_longest_seq"])
    return res_df


def get_ccxt_price_df(
    ccxt_universe: Dict[str, List[str]],
    ccxt_loader: imccdaloloa.CcxtLoader,
    config: ccocon.Config,
) -> pd.DataFrame:
    """
    Read price data from CCXT for a given universe using the given loader.

    :param ccxt_universe: CCXT trade universe
    :param ccxt_loader: CCXT loader
    :param config: parameters config
    :return: price data for a given universe
    """
    # Initialize lists of column names and returns series.
    colnames = []
    price_srs_list = []
    # Iterate over exchange ids and currency pairs.
    for exchange_id in ccxt_universe:
        for curr_pair in ccxt_universe[exchange_id]:
            # Construct a colname from exchange id and currency pair.
            colname = " ".join([exchange_id, curr_pair])
            colnames.append(colname)
            # Extract historical data.
            data = ccxt_loader.read_data_from_filesystem(
                exchange_id=exchange_id,
                currency_pair=curr_pair,
                data_type="OHLCV",
            )
            # Get series of prices and append to the list.
            price_srs = data[config["data"]["close_price_col_name"]]
            price_srs_list.append(price_srs)
    # Construct a dataframe and assign column names.
    df = pd.concat(price_srs_list, axis=1)
    df.columns = colnames
    # Resample to the specified frequency.
    df = df.resample(config["data"]["freq"], closed="right", label="right").mean()
    return df
