"""
Compute crypto-related statistics.

Import as:

import research.cc.statistics as rccsta
"""
import logging
from typing import Callable, List, Union

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
    Compute start-end table for exchange-currency data.

    Note: `price_data` must be resampled using NaNs.

    Start-end table's structure is:
        - exchange name
        - currency pair
        - minimum observed timestamp
        - maximum observed timestamp
        - the number of not NaN data points
        - data coverage, which is the number of not NaN data points divided
          by the number of all data points as percentage
        - the number of days for which data is available
        - the average number of not NaN data points per day
        - the number of days of the longest not-NaN sequence
        - the share of the longest not-NaN sequence in data
        - start date of the longest not-NaN sequence
        - end data of the longest not-NaN sequence


    :param price_data: crypto price data
    :param config: parameters config
    :return: start-end table
    """
    hdbg.dassert_is_subset(
        [
            config["column_names"]["close_price"],
            config["column_names"]["currency_pair"],
            config["column_names"]["exchange_id"],
        ],
        price_data.columns,
    )
    hdbg.dassert_isinstance(price_data.index, pd.DatetimeIndex)
    hhpandas.dassert_monotonic_index(price_data.index)
    hdbg.dassert_eq(price_data.index.freq, "T")
    # Get series of close price.
    close_price_srs = price_data[config["column_names"]["close_price"]]
    # Remove leading and trailing NaNs.
    first_idx = close_price_srs.first_valid_index()
    last_idx = close_price_srs.last_valid_index()
    close_price_srs = close_price_srs[first_idx:last_idx].copy()
    # Get the longest not-NaN sequence in the close price series.
    longest_not_nan_seq = find_longest_not_nan_sequence(close_price_srs)
    # Compute necessary stats and put them a dataframe.
    res_df = pd.DataFrame(dtype="object")
    res_df["min_timestamp"] = first_idx
    res_df["max_timestamp"] = last_idx
    res_df["n_data_points"] = close_price_srs.count()
    res_df["coverage"] = round(
        (1 - csta.compute_frac_nan(close_price_srs)) * 100, 2
    )
    res_df["days_available"] = (last_idx - first_idx).days
    res_df["avg_data_points_per_day"] = round(
        res_df["n_data_points"] / res_df["days_available"], 2
    )
    res_df["longest_not_nan_seq_days"] = len(longest_not_nan_seq)
    res_df["longest_not_nan_seq_share"] = round(
        len(longest_not_nan_seq) / len(close_price_srs), 2
    )
    res_df["longest_not_nan_seq_start_date"] = longest_not_nan_seq.index[0]
    res_df["longest_not_nan_seq_end_date"] = longest_not_nan_seq.index[-1]
    return start_end_table


# TODO(Grisha): move `get_loader_for_vendor` out in #269.
# TODO(Grisha): use the abstract class in #313.
def get_loader_for_vendor(
    config: ccocon.Config
) -> Union[imccdaloloa.CcxtLoader, imcrdaloloa.CddLoader]:
    """
    Get vendor specific loader instance.

    :param config: config
    :return: loader instance
    """
    vendor = config["data"]["vendor"]
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
    vendor_universe: List[imdauni.ExchangeCurrencyTuple],
    config: ccocon.Config,
    stats_func: Callable,
) -> pd.DataFrame:
    """
    Compute stats on the vendor universe level.

    E.g., to compute start-end table for the universe do:
    `compute_stats_for_universe(vendor_universe, config, compute_start_end_table, config)`.

    :param vendor_universe: vendor universe as a list of of exchange-currency tuples
    :param config: parameters config
    :param stats_func: function to compute statistics, e.g. `compute_start_end_table`
    :return: stats table for all exchanges and currencies in the vendor universe
    """
    hdbg.dassert_isinstance(stats_func, Callable)
    # Initialize loader.
    loader = get_loader_for_vendor(config)
    # Iterate over vendor universe tuples.
    for exchange_id, currency_pair in vendor_universe:
        # Read data for current vendor, exchange, currency pair.
        data = loader.read_data_from_filesystem(
            exchange_id,
            currency_pair,
            config["data"]["data_type"],
        )
        # Compute stats on the exchange-currency level.
        cur_stats_data = stats_func(data)
        cur_stats_data["vendor"] = config["data"]["vendor"]
        stats_data.append(cur_stats_data)
    # Concatenate the results.
    stats_table = pd.concat(stats_data, ignore_index=True)
    # Sort if columns to sort by are specified.
    if config["column_names"]["columns_to_sort_by"]:
        stats_table = stats_table.sort_values(
            by=config["column_names"]["columns_to_sort_by"]
        )
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
