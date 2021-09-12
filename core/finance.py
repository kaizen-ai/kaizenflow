"""
Basic functions processing financial data.

Import as:

import core.finance as cofinanc
"""

import datetime
import logging
from typing import Dict, List, Optional, Tuple, Union, cast

import numpy as np
import pandas as pd
import statsmodels.api as sm

import core.signal_processing as csigproc
import helpers.dataframe as hdatafr
import helpers.dbg as hdbg
import helpers.hpandas as hpandas
import helpers.htypes as htypes
import helpers.printing as hprint

_LOG = logging.getLogger(__name__)


def remove_dates_with_no_data(
    df: pd.DataFrame, report_stats: bool
) -> pd.DataFrame:
    """
    Given a df indexed with timestamps, scan the data by date and filter out
    all the data when it's all nans.

    :param report_stats: if True report information about the performed
        operation
    :return: filtered df
    """
    # This is not strictly necessary.
    hpandas.dassert_strictly_increasing_index(df)
    # Store the dates of the days removed because of all NaNs.
    removed_days = []
    # Accumulate the df for all the days that are not discarded.
    df_out = []
    # Store the days processed.
    num_days = 0
    # Scan the df by date.
    for date, df_tmp in df.groupby(df.index.date):
        if np.isnan(df_tmp).all(axis=1).all():
            _LOG.debug("No data on %s", date)
            removed_days.append(date)
        else:
            df_out.append(df_tmp)
        num_days += 1
    df_out = pd.concat(df_out)
    hpandas.dassert_strictly_increasing_index(df_out)
    #
    if report_stats:
        # Stats for rows.
        _LOG.info("df.index in [%s, %s]", df.index.min(), df.index.max())
        removed_perc = hprint.perc(df.shape[0] - df_out.shape[0], df.shape[0])
        _LOG.info("Rows removed: %s", removed_perc)
        # Stats for all days.
        removed_perc = hprint.perc(len(removed_days), num_days)
        _LOG.info("Number of removed days: %s", removed_perc)
        # Find week days.
        removed_weekdays = [d for d in removed_days if d.weekday() < 5]
        removed_perc = hprint.perc(len(removed_weekdays), len(removed_days))
        _LOG.info("Number of removed weekdays: %s", removed_perc)
        _LOG.info("Weekdays removed: %s", ", ".join(map(str, removed_weekdays)))
        # Stats for weekend days.
        removed_perc = hprint.perc(
            len(removed_days) - len(removed_weekdays), len(removed_days)
        )
        _LOG.info("Number of removed weekend days: %s", removed_perc)
    return df_out


# TODO(gp): Active trading hours and days are specific of different futures.
#  Consider explicitly passing this information instead of using defaults.
def set_non_ath_to_nan(
    df: pd.DataFrame,
    start_time: Optional[datetime.time] = None,
    end_time: Optional[datetime.time] = None,
) -> pd.DataFrame:
    """
    Filter according to active trading hours.

    We assume time intervals are:
    - left closed, right open `[a, b)`
    - labeled right

    Row is not set to `np.nan` iff its `time` satisfies:
      - `start_time < time`, and
      - `time <= end_time`
    """
    hdbg.dassert_isinstance(df.index, pd.DatetimeIndex)
    hpandas.dassert_strictly_increasing_index(df)
    if start_time is None:
        start_time = datetime.time(9, 30)
    if end_time is None:
        end_time = datetime.time(16, 0)
    hdbg.dassert_lte(start_time, end_time)
    # Compute the indices to remove.
    times = df.index.time
    to_remove_mask = (times <= start_time) | (end_time < times)
    # Make a copy and filter.
    df = df.copy()
    df[to_remove_mask] = np.nan
    return df


def remove_times_outside_window(
    df: pd.DataFrame,
    start_time: datetime.time,
    end_time: datetime.time,
    bypass: bool = False,
) -> pd.DataFrame:
    """
    Remove times outside of (start_time, end_time].
    """
    # Perform sanity checks.
    hdbg.dassert_isinstance(df.index, pd.DatetimeIndex)
    hpandas.dassert_strictly_increasing_index(df)
    hdbg.dassert_isinstance(start_time, datetime.time)
    hdbg.dassert_isinstance(end_time, datetime.time)
    hdbg.dassert_lte(start_time, end_time)
    if bypass:
        return df
    # Compute the indices to remove.
    times = df.index.time
    to_remove_mask = (times <= start_time) | (end_time < times)
    return df[~to_remove_mask]


def set_weekends_to_nan(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter out weekends setting the corresponding values to `np.nan`.
    """
    hdbg.dassert_isinstance(df.index, pd.DatetimeIndex)
    # 5 = Saturday, 6 = Sunday.
    to_remove_mask = df.index.dayofweek.isin([5, 6])
    df = df.copy()
    df[to_remove_mask] = np.nan
    return df


def remove_weekends(df: pd.DataFrame, bypass: bool = False) -> pd.DataFrame:
    """
    Remove weekends from `df`.
    """
    hdbg.dassert_isinstance(df.index, pd.DatetimeIndex)
    # 5 = Saturday, 6 = Sunday.
    if bypass:
        return df
    to_remove_mask = df.index.dayofweek.isin([5, 6])
    return df[~to_remove_mask]


# #############################################################################
# Resampling.
# #############################################################################

# TODO(Paul): Consider moving resampling code to a new `resampling.py`


def compute_vwap(
    df: pd.DataFrame,
    *,
    rule: str,
    price_col: str,
    volume_col: str,
    offset: Optional[str] = None,
) -> pd.Series:
    """
    Compute VWAP from price and volume columns.

    :param df: input dataframe with datetime index
    :param rule: resampling frequency and VWAP aggregation window
    :param price_col: price for bar
    :param volume_col: volume for bar
    :param offset: offset in the Pandas format (e.g., `1T`) used to shift the
        sampling
    :return: vwap price series
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert_in(price_col, df.columns)
    hdbg.dassert_in(volume_col, df.columns)
    # Only use rows where both price and volume are non-NaN.
    non_nan_idx = df[[price_col, volume_col]].dropna().index
    nan_idx = df.index.difference(non_nan_idx)
    price = df[price_col]
    price.loc[nan_idx] = np.nan
    volume = df[volume_col]
    volume.loc[nan_idx] = np.nan
    # Weight price according to volume.
    volume_weighted_price = price.multiply(volume)
    resampled_volume_weighted_price = csigproc.resample(
        volume_weighted_price,
        rule=rule,
        offset=offset,
    ).sum(min_count=1)
    resampled_volume = csigproc.resample(volume, rule=rule, offset=offset).sum(
        min_count=1
    )
    # Complete the VWAP calculation.
    vwap = resampled_volume_weighted_price.divide(resampled_volume)
    # Replace infs with NaNs.
    vwap = vwap.replace([-np.inf, np.inf], np.nan)
    vwap.name = "vwap"
    return vwap


def _resample_with_aggregate_function(
    df: pd.DataFrame,
    rule: str,
    cols: List[str],
    agg_func: str,
    agg_func_kwargs: htypes.Kwargs,
) -> pd.DataFrame:
    """
    Resample columns `cols` of `df` using the passed parameters.
    """
    hdbg.dassert(not df.empty)
    hdbg.dassert_isinstance(cols, list)
    hdbg.dassert(cols, msg="`cols` must be nonempty.")
    hdbg.dassert_is_subset(cols, df.columns)
    resampler = csigproc.resample(df[cols], rule=rule)
    resampled = resampler.agg(agg_func, **agg_func_kwargs)
    return resampled


def resample_bars(
    df: pd.DataFrame,
    rule: str,
    resampling_groups: List[Tuple[Dict[str, str], str, htypes.Kwargs]],
    vwap_groups: List[Tuple[str, str, str]],
) -> pd.DataFrame:
    """
    Resampling with optional VWAP.

    Output column names must not collide.

    :param resampling_groups: list of tuples of the following form:
        (col_dict, aggregation function, aggregation kwargs)
    :param vwap_groups: list of tuples of the following form:
        (in_price_col_name, in_vol_col_name, vwap_col_name)
    """
    results = []
    for col_dict, agg_func, agg_func_kwargs in resampling_groups:
        resampled = _resample_with_aggregate_function(
            df,
            rule=rule,
            cols=list(col_dict.keys()),
            agg_func=agg_func,
            agg_func_kwargs=agg_func_kwargs,
        )
        resampled = resampled.rename(columns=col_dict)
        hdbg.dassert(not resampled.columns.has_duplicates)
        results.append(resampled)
    for price_col, volume_col, vwap_col in vwap_groups:
        vwap = compute_vwap(
            df, rule=rule, price_col=price_col, volume_col=volume_col
        )
        vwap.name = vwap_col
        results.append(vwap)
    out_df = pd.concat(results, axis=1)
    hdbg.dassert(not out_df.columns.has_duplicates)
    hdbg.dassert(out_df.index.freq)
    return out_df


# TODO(Paul): Consider deprecating.
# This provides some sensible defaults for `resample_bars()`, but may not be
# worth the additional complexity.
def resample_time_bars(
    df: pd.DataFrame,
    rule: str,
    *,
    return_cols: Optional[List[str]] = None,
    return_agg_func: Optional[str] = None,
    return_agg_func_kwargs: Optional[htypes.Kwargs] = None,
    price_cols: Optional[List[str]] = None,
    price_agg_func: Optional[str] = None,
    price_agg_func_kwargs: Optional[htypes.Kwargs] = None,
    volume_cols: Optional[List[str]] = None,
    volume_agg_func: Optional[str] = None,
    volume_agg_func_kwargs: Optional[htypes.Kwargs] = None,
) -> pd.DataFrame:
    """
    Convenience resampler for time bars.

    Features:
    - Respects causality
    - Chooses sensible defaults:
      - returns are summed
      - prices are averaged
      - volume is summed
    - NaN intervals remain NaN
    - Defaults may be overridden (e.g., choose `last` instead of `mean` for
      price)

    :param df: input dataframe with datetime index
    :param rule: resampling frequency with pandas convention (e.g., "5T")
    :param return_cols: columns containing returns
    :param return_agg_func: aggregation function, default is "sum"
    :param return_agg_func_kwargs: kwargs
    :param price_cols: columns containing price
    :param price_agg_func: aggregation function, default is "mean"
    :param price_agg_func_kwargs: kwargs
    :param volume_cols: columns containing volume
    :param volume_agg_func: aggregation function, default is "sum"
    :param volume_agg_func_kwargs: kwargs
    :return: dataframe of resampled time bars with columns from `*_cols` variables,
        although not in the same order as passed
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    resampling_groups = []
    if return_cols:
        col_mapping = {col: col for col in return_cols}
        agg_func = return_agg_func or "sum"
        agg_func_kwargs = return_agg_func_kwargs or {"min_count": 1}
        group = (col_mapping, agg_func, agg_func_kwargs)
        resampling_groups.append(group)
    if price_cols:
        col_mapping = {col: col for col in price_cols}
        agg_func = price_agg_func or "mean"
        agg_func_kwargs = price_agg_func_kwargs or {}
        group = (col_mapping, agg_func, agg_func_kwargs)
        resampling_groups.append(group)
    if volume_cols:
        col_mapping = {col: col for col in volume_cols}
        agg_func = volume_agg_func or "sum"
        agg_func_kwargs = volume_agg_func_kwargs or {"min_count": 1}
        group = (col_mapping, agg_func, agg_func_kwargs)
        resampling_groups.append(group)
    result_df = resample_bars(
        df,
        rule=rule,
        resampling_groups=resampling_groups,
        vwap_groups=[],
    )
    return result_df


def resample_ohlcv_bars(
    df: pd.DataFrame,
    rule: str,
    *,
    open_col: Optional[str] = "open",
    high_col: Optional[str] = "high",
    low_col: Optional[str] = "low",
    close_col: Optional[str] = "close",
    volume_col: Optional[str] = "volume",
    add_twap_vwap: bool = False,
) -> pd.DataFrame:
    """
    Resample OHLCV bars and optionally add TWAP, VWAP prices based on "close".

    TODO(Paul): compare to native pandas `ohlc` resampling.

    :param df: input dataframe with datetime index
    :param rule: resampling frequency
    :param open_col: name of "open" column
    :param high_col: name of "high" column
    :param low_col: name of "low" column
    :param close_col: name of "close" column
    :param volume_col: name of "volume" column
    :param add_twap_vwap: if `True`, add "twap" and "vwap" columns
    :return: resampled OHLCV dataframe with same column names; if
        `add_twap_vwap`, then also includes "twap" and "vwap" columns.
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    # Make sure that requested OHLCV columns are present in the dataframe.
    for col in [open_col, high_col, low_col, close_col, volume_col]:
        if col is not None:
            hdbg.dassert_in(col, df.columns)
    resampling_groups = []
    if open_col:
        group = ({open_col: open_col}, "first", {})
        resampling_groups.append(group)
    if high_col:
        group = ({high_col: high_col}, "max", {})
        resampling_groups.append(group)
    if low_col:
        group = ({low_col: low_col}, "min", {})
        resampling_groups.append(group)
    if close_col:
        group = ({close_col: close_col}, "last", {})
        resampling_groups.append(group)
    if volume_col:
        group = ({volume_col: volume_col}, "sum", {"min_count": 1})
        resampling_groups.append(group)
    result_df = resample_bars(
        df,
        rule=rule,
        resampling_groups=resampling_groups,
        vwap_groups=[],
    )
    # TODO(Paul): Refactor this so that we do not call `compute_twap_vwap()`
    #  directly.
    # Add TWAP / VWAP prices, if needed.
    if add_twap_vwap:
        close_col = cast(str, close_col)
        volume_col = cast(str, volume_col)
        twap_vwap_df = compute_twap_vwap(
            df, rule=rule, price_col=close_col, volume_col=volume_col
        )
        result_df = result_df.merge(
            twap_vwap_df, how="outer", left_index=True, right_index=True
        )
        hdbg.dassert(result_df.index.freq)
    return result_df


# TODO(Paul): Deprecate this function. The bells and whistles do not really
# fit, and the core functionality can be accessed through the above functions.
def compute_twap_vwap(
    df: pd.DataFrame,
    rule: str,
    *,
    price_col: str,
    volume_col: str,
    offset: Optional[str] = None,
    add_bar_volume: bool = False,
    add_bar_start_timestamps: bool = False,
    add_epoch: bool = False,
    add_last_price: bool = False,
) -> pd.DataFrame:
    """
    Compute TWAP/VWAP from price and volume columns.

    :param df: input dataframe with datetime index
    :param rule: resampling frequency and TWAP/VWAP aggregation window
    :param price_col: price for bar
    :param volume_col: volume for bar
    :param offset: offset in the Pandas format (e.g., `1T`) used to shift the
        sampling
    :return: twap and vwap price series
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    # TODO(*): Determine whether we really need this. Disabling for now to
    #  accommodate data that is not perfectly aligned with a pandas freq
    #  (e.g., Kibot).
    # hdbg.dassert(df.index.freq)
    vwap = compute_vwap(
        df, rule=rule, price_col=price_col, volume_col=volume_col, offset=offset
    )
    price = df[price_col]
    # Calculate TWAP, but preserve NaNs for all-NaN bars.
    twap = csigproc.resample(price, rule=rule, offset=offset).mean()
    twap.name = "twap"
    dfs = [vwap, twap]
    if add_last_price:
        # Calculate last price (regardless of whether we have volume data).
        last_price = csigproc.resample(price, rule=rule, offset=offset).last(
            min_count=1
        )
        last_price.name = "last"
        dfs.append(last_price)
    if add_bar_volume:
        volume = df[volume_col]
        # Calculate bar volume (regardless of whether we have price data).
        bar_volume = csigproc.resample(volume, rule=rule, offset=offset).sum(
            min_count=1
        )
        bar_volume.name = "volume"
        dfs.append(bar_volume)
    if add_bar_start_timestamps:
        bar_start_timestamps = compute_bar_start_timestamps(vwap)
        dfs.append(bar_start_timestamps)
    if add_epoch:
        epoch = compute_epoch(vwap)
        dfs.append(epoch)
    df_out = pd.concat(dfs, axis=1)
    return df_out


def compute_bar_start_timestamps(
    data: Union[pd.Series, pd.DataFrame],
) -> pd.Series:
    """
    Given data on a uniform grid indexed by end times, return start times.

    :param data: a dataframe or series with a `DatetimeIndex` that has a `freq`.
        It is assumed that the timestamps in the index are times corresponding
        to the end of bars. For this particular function, assumptions around
        which endpoints are open or closed are not important.
    :return: a series with index `data.index` (of bar end timestamps) and values
        equal to bar start timestamps
    """
    freq = data.index.freq
    hdbg.dassert(freq, msg="DatetimeIndex must have a frequency.")
    size = data.index.size
    hdbg.dassert_lte(1, size, msg="DatetimeIndex has size=%i values" % size)
    date_range = data.index.shift(-1)
    srs = pd.Series(index=data.index, data=date_range, name="bar_start_timestamp")
    return srs


def compute_epoch(
    data: Union[pd.Series, pd.DataFrame], unit: Optional[str] = None
) -> Union[pd.Series, pd.DataFrame]:
    """
    Convert datetime index times to minutes, seconds, or nanoseconds.

    TODO(Paul): Add unit tests.

    :param data: a dataframe or series with a `DatetimeIndex`
    :param unit: unit for reporting epoch. Supported units are:
        "minute", "second', "nanosecond". Default is "minute".
    :return: series of int64's with epoch in minutes, seconds, or nanoseconds
    """
    unit = unit or "minute"
    hdbg.dassert_isinstance(data.index, pd.DatetimeIndex)
    nanoseconds = data.index.view(np.int64)
    if unit == "minute":
        epochs = np.int64(nanoseconds * 1e-9 / 60)
    elif unit == "second":
        epochs = np.int64(nanoseconds * 1e-9)
    elif unit == "nanosecond":
        epochs = nanoseconds
    else:
        raise ValueError(
            f"Unsupported unit=`{unit}`. Supported units are "
            "'minute', 'second', and 'nanosecond'."
        )
    srs = pd.Series(index=data.index, data=epochs, name=unit)
    if isinstance(data, pd.DataFrame):
        return srs.to_frame()
    return srs


# #############################################################################
# Bid-ask processing.
# #############################################################################


def process_bid_ask(
    df: pd.DataFrame,
    bid_col: str,
    ask_col: str,
    bid_volume_col: str,
    ask_volume_col: str,
    requested_cols: Optional[List[str]] = None,
    join_output_with_input: bool = False,
) -> pd.DataFrame:
    """
    Process top-of-book bid/ask quotes.

    :param df: dataframe with columns for top-of-book bid/ask info
    :param bid_col: bid price column
    :param ask_col: ask price column
    :param bid_volume_col: column with quoted volume at bid
    :param ask_volume_col: column with quoted volume at ask
    :param requested_cols: the requested output columns; `None` returns all
        available.
    :param join_output_with_input: whether to only return the requested columns
        or to join the requested columns to the input dataframe
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert_in(bid_col, df.columns)
    hdbg.dassert_in(ask_col, df.columns)
    hdbg.dassert_in(bid_volume_col, df.columns)
    hdbg.dassert_in(ask_volume_col, df.columns)
    hdbg.dassert(not (df[bid_col] > df[ask_col]).any())
    supported_cols = [
        "mid",
        "geometric_mid",
        "quoted_spread",
        "relative_spread",
        "log_relative_spread",
        "weighted_mid",
        # These imbalances are with respect to shares.
        "order_book_imbalance",
        "centered_order_book_imbalance",
        "log_order_book_imbalance",
        # TODO: use `notional` instead of `value`.
        "bid_value",
        "ask_value",
        "mid_value",
    ]
    requested_cols = requested_cols or supported_cols
    hdbg.dassert_is_subset(
        requested_cols,
        supported_cols,
        "The available columns to request are %s",
        supported_cols,
    )
    hdbg.dassert(requested_cols)
    requested_cols = set(requested_cols)
    results = []
    if "mid" in requested_cols:
        srs = ((df[bid_col] + df[ask_col]) / 2).rename("mid")
        results.append(srs)
    if "geometric_mid" in requested_cols:
        srs = np.sqrt(df[bid_col] * df[ask_col]).rename("geometric_mid")
        results.append(srs)
    if "quoted_spread" in requested_cols:
        srs = (df[ask_col] - df[bid_col]).rename("quoted_spread")
        results.append(srs)
    if "relative_spread" in requested_cols:
        srs = 2 * (df[ask_col] - df[bid_col]) / (df[ask_col] + df[bid_col])
        srs = srs.rename("relative_spread")
        results.append(srs)
    if "log_relative_spread" in requested_cols:
        srs = (np.log(df[ask_col]) - np.log(df[bid_col])).rename(
            "log_relative_spread"
        )
        results.append(srs)
    if "weighted_mid" in requested_cols:
        srs = (
            df[bid_col] * df[ask_volume_col] + df[ask_col] * df[bid_volume_col]
        ) / (df[ask_volume_col] + df[bid_volume_col])
        srs = srs.rename("weighted_mid")
        results.append(srs)
    if "order_book_imbalance" in requested_cols:
        srs = df[bid_volume_col] / (df[bid_volume_col] + df[ask_volume_col])
        srs = srs.rename("order_book_imbalance")
        results.append(srs)
    if "centered_order_book_imbalance" in requested_cols:
        srs = (df[bid_volume_col] - df[ask_volume_col]) / (
            df[bid_volume_col] + df[ask_volume_col]
        )
        srs = srs.rename("centered_order_book_imbalance")
        results.append(srs)
    if "log_order_book_imbalance" in requested_cols:
        srs = np.log(df[bid_volume_col]) - np.log(df[ask_volume_col])
        srs = srs.rename("log_order_book_imbalance")
        results.append(srs)
    if "bid_value" in requested_cols:
        srs = (df[bid_col] * df[bid_volume_col]).rename("bid_value")
        results.append(srs)
    if "ask_value" in requested_cols:
        srs = (df[ask_col] * df[ask_volume_col]).rename("ask_value")
        results.append(srs)
    if "mid_value" in requested_cols:
        srs = (
            df[bid_col] * df[bid_volume_col] + df[ask_col] * df[ask_volume_col]
        ) / 2
        srs = srs.rename("mid_value")
        results.append(srs)
    out_df = pd.concat(results, axis=1)
    # TODO(gp): Maybe factor out this in a `_maybe_join_output_with_input` since
    #  it seems a common idiom.
    if join_output_with_input:
        out_df = out_df.merge(df, left_index=True, right_index=True, how="outer")
        hdbg.dassert(not out_df.columns.has_duplicates)
    return out_df


def compute_spread_cost(
    df: pd.DataFrame,
    # TODO(gp): -> position_intent_1_col or position_intent_col ?
    target_position_col: str,
    spread_col: str,
    spread_fraction_paid: float,
    *,
    join_output_with_input: bool = False,
) -> pd.DataFrame:
    """
    Compute spread costs incurred by changing position values.

    The columns are aligned so that

    :param target_position_col: series of one-step-ahead target positions
    :param spread_col: series of spreads
    :param spread_fraction_paid: number indicating the fraction of the spread
        paid, e.g., `0.5` means that 50% of the spread is paid
    """
    # TODO(gp): Clarify / make uniform the spread nomenclature. If `spread_col` is
    #  `quoted_spread` then midpoint corresponds to 0.5.
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert_in(target_position_col, df.columns)
    hdbg.dassert_in(spread_col, df.columns)
    # if spread_fraction_paid < 0:
    #    _LOG.warning("spread_fraction_paid=%f", spread_fraction_paid)
    # hdbg.dassert_lte(0, spread_fraction_paid)
    hdbg.dassert_lte(spread_fraction_paid, 1)
    # TODO(gp): adjusted_spread -> spread_paid?
    adjusted_spread = spread_fraction_paid * df[spread_col]
    # Since target_
    target_position_delta = df[target_position_col].diff().shift(1)
    spread_costs = target_position_delta.abs().multiply(adjusted_spread)
    out_df = spread_costs.rename("spread_cost").to_frame()
    if join_output_with_input:
        out_df = out_df.merge(df, left_index=True, right_index=True, how="outer")
        hdbg.dassert(not out_df.columns.has_duplicates)
    return out_df


def compute_pnl(
    df: pd.DataFrame,
    position_intent_col: str,
    return_col: str,
    *,
    join_output_with_input: bool = False,
) -> pd.DataFrame:
    """
    Compute PnL from a stream of position intents and returns.

    :param position_intent_col: series of one-step-ahead target positions
    :param return_col: series of returns
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert_in(position_intent_col, df.columns)
    hdbg.dassert_in(return_col, df.columns)
    #
    pnl = df[position_intent_col].shift(2).multiply(df[return_col])
    out_df = pnl.rename("pnl").to_frame()
    if join_output_with_input:
        out_df = out_df.merge(df, left_index=True, right_index=True, how="outer")
        hdbg.dassert(not out_df.columns.has_duplicates)
    return out_df


def compute_spread_cost(
    df: pd.DataFrame,
    position_col: str,
    spread_col: str,
    spread_fraction_paid: float,
    position_delay: int = 0,
    spread_delay: int = 1,
    join_output_with_input: bool = False,
) -> pd.DataFrame:
    """
    Compute spread costs incurred by changing position values.

    The default delays assume that timestamped positions and spreads represent
    the state of the world at that timestamp.

    :param position_col: series of positions
    :param spread_col: series of spreads
    :param spread_fraction_paid: number indicating the fraction of the spread
        paid, e.g., `0.5` means that 50% of the spread is paid
    :param position_delay: number of shifts to pre-apply to `position_col`
    :param spread_delay: number of shifts to pre-apply to `spread_col`
    """
    dbg.dassert_isinstance(df, pd.DataFrame)
    dbg.dassert_in(position_col, df.columns)
    dbg.dassert_in(spread_col, df.columns)
    dbg.dassert_lte(0, spread_fraction_paid)
    dbg.dassert_lte(spread_fraction_paid, 1)
    adjusted_spread = spread_fraction_paid * df[spread_col].shift(spread_delay)
    position_delta = df[position_col].shift(position_delay).diff()
    spread_costs = position_delta.abs().multiply(adjusted_spread)
    out_df = spread_costs.rename("spread_cost").to_frame()
    if join_output_with_input:
        out_df = out_df.merge(df, left_index=True, right_index=True, how="outer")
        dbg.dassert(not out_df.columns.has_duplicates)
    return out_df


# #############################################################################
# Returns calculation and helpers.
# #############################################################################


def compute_ret_0(
    prices: Union[pd.Series, pd.DataFrame], mode: str
) -> Union[pd.Series, pd.DataFrame]:
    if mode == "pct_change":
        ret_0 = prices.divide(prices.shift(1)) - 1
    elif mode == "log_rets":
        ret_0 = np.log(prices) - np.log(prices.shift(1))
    elif mode == "diff":
        # TODO(gp): Use shifts for clarity, e.g.,
        # ret_0 = prices - prices.shift(1)
        ret_0 = prices.diff()
    else:
        raise ValueError("Invalid mode='%s'" % mode)
    if isinstance(ret_0, pd.Series):
        ret_0.name = "ret_0"
    return ret_0


def compute_ret_0_from_multiple_prices(
    prices: Dict[str, pd.DataFrame], col_name: str, mode: str
) -> pd.DataFrame:
    hdbg.dassert_isinstance(prices, dict)
    rets = []
    for s, price_df in prices.items():
        _LOG.debug("Processing s=%s", s)
        rets_tmp = compute_ret_0(price_df[col_name], mode)
        rets_tmp = pd.DataFrame(rets_tmp)
        rets_tmp.columns = ["%s_ret_0" % s]
        rets.append(rets_tmp)
    rets = pd.concat(rets, sort=True, axis=1)
    return rets


# TODO(*): Add a decorator for handling multi-variate prices as in
#  https://github.com/.../.../issues/568


def compute_prices_from_rets(
    price: pd.Series,
    rets: pd.Series,
    mode: str,
) -> pd.Series:
    """
    Compute price p_1 at moment t_1 with given price p_0 at t_0 and return
    ret_1.

    This implies that input has ret_1 at moment t_1 and uses price p_0 from
    previous step t_0. If we have forward returns instead (ret_1 and p_0 are at
    t_0), we need to shift input returns index one step ahead.

    :param price: series with prices
    :param rets: series with returns
    :param mode: returns mode as in compute_ret_0
    :return: series with computed prices
    """
    hdbg.dassert_isinstance(price, pd.Series)
    hdbg.dassert_isinstance(rets, pd.Series)
    price = price.reindex(rets.index).shift(1)
    if mode == "pct_change":
        price_pred = price * (rets + 1)
    elif mode == "log_rets":
        price_pred = price * np.exp(rets)
    elif mode == "diff":
        price_pred = price + rets
    else:
        raise ValueError("Invalid mode='%s'" % mode)
    return price_pred


def convert_log_rets_to_pct_rets(
    log_rets: Union[float, pd.Series, pd.DataFrame]
) -> Union[float, pd.Series, pd.DataFrame]:
    """
    Convert log returns to percentage returns.

    :param log_rets: time series of log returns
    :return: time series of percentage returns
    """
    return np.exp(log_rets) - 1


def convert_pct_rets_to_log_rets(
    pct_rets: Union[float, pd.Series, pd.DataFrame]
) -> Union[float, pd.Series, pd.DataFrame]:
    """
    Convert percentage returns to log returns.

    :param pct_rets: time series of percentage returns
    :return: time series of log returns
    """
    return np.log(pct_rets + 1)


def rescale_to_target_annual_volatility(
    srs: pd.Series, volatility: float
) -> pd.Series:
    """
    Rescale srs to achieve target annual volatility.

    NOTE: This is not a causal rescaling, but SR is an invariant.

    :param srs: returns series. Index must have `freq`.
    :param volatility: annualized volatility as a proportion (e.g., `0.1`
        corresponds to 10% annual volatility)
    :return: rescaled returns series
    """
    hdbg.dassert_isinstance(srs, pd.Series)
    scale_factor = compute_volatility_normalization_factor(
        srs, target_volatility=volatility
    )
    return scale_factor * srs


def compute_inverse_volatility_weights(df: pd.DataFrame) -> pd.Series:
    """
    Calculate inverse volatility relative weights.

    :param df: cols contain log returns
    :return: series of weights
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert(not df.columns.has_duplicates)
    # Compute inverse volatility weights.
    # The result of `compute_volatility_normalization_factor()`
    # is independent of the `target_volatility`.
    weights = df.apply(
        lambda x: compute_volatility_normalization_factor(
            x, target_volatility=0.1
        )
    )
    # Replace inf's with 0's in weights.
    weights.replace([np.inf, -np.inf], np.nan, inplace=True)
    # Rescale weights to percentages.
    weights /= weights.sum()
    weights.name = "weights"
    # Replace NaN with zero for weights.
    weights = hdatafr.apply_nan_mode(weights, mode="fill_with_zero")
    return weights


def aggregate_log_rets(df: pd.DataFrame, weights: pd.Series) -> pd.Series:
    """
    Compute aggregate log returns.

    :param df: cols contain log returns
    :param weights: series of weights
    :return: series of log returns
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert(not df.columns.has_duplicates)
    hdbg.dassert(df.columns.equals(weights.index))
    df = df.apply(
        lambda x: rescale_to_target_annual_volatility(x, weights[x.name])
    )
    df = df.apply(convert_log_rets_to_pct_rets)
    df = df.mean(axis=1)
    srs = df.squeeze()
    srs = convert_pct_rets_to_log_rets(srs)
    return srs


def compute_volatility_normalization_factor(
    srs: pd.Series, target_volatility: float
) -> float:
    """
    Compute scale factor of a series according to a target volatility.

    :param srs: returns series. Index must have `freq`.
    :param target_volatility: target volatility as a proportion (e.g., `0.1`
        corresponds to 10% annual volatility)
    :return: scale factor
    """
    hdbg.dassert_isinstance(srs, pd.Series)
    # TODO(Paul): Determine how to deal with no `freq`.
    ppy = hdatafr.infer_sampling_points_per_year(srs)
    srs = hdatafr.apply_nan_mode(srs, mode="fill_with_zero")
    scale_factor: float = target_volatility / (np.sqrt(ppy) * srs.std())
    _LOG.debug("scale_factor=%f", scale_factor)
    return scale_factor


# TODO(*): Consider moving to `statistics.py`.
# #############################################################################
# Returns stats.
# #############################################################################


def compute_kratio(log_rets: pd.Series) -> float:
    """
    Calculate K-Ratio of a time series of log returns.

    :param log_rets: time series of log returns
    :return: K-Ratio
    """
    hdbg.dassert_isinstance(log_rets, pd.Series)
    log_rets = maybe_resample(log_rets)
    log_rets = hdatafr.apply_nan_mode(log_rets, mode="fill_with_zero")
    cum_rets = log_rets.cumsum()
    # Fit the best line to the daily rets.
    x = range(len(cum_rets))
    x = sm.add_constant(x)
    reg = sm.OLS(cum_rets, x)
    model = reg.fit()
    # Compute k-ratio as slope / std err of slope.
    kratio = model.params[1] / model.bse[1]
    # Adjust k-ratio by the number of observations and points per year.
    ppy = hdatafr.infer_sampling_points_per_year(log_rets)
    kratio = kratio * np.sqrt(ppy) / len(log_rets)
    kratio = cast(float, kratio)
    return kratio


def compute_drawdown(pnl: pd.Series) -> pd.Series:
    """
    Calculate drawdown of a time series of per-period PnL.

    At each point in time, drawdown is either zero (a new high-water mark is
    achieved), or positive, measuring the cumulative loss since the last
    high-water mark.

    According to our conventions, drawdown is always nonnegative.

    :param pnl: time series of per-period PnL
    :return: drawdown time series
    """
    hdbg.dassert_isinstance(pnl, pd.Series)
    pnl = hdatafr.apply_nan_mode(pnl, mode="fill_with_zero")
    cum_pnl = pnl.cumsum()
    running_max = np.maximum.accumulate(cum_pnl)  # pylint: disable=no-member
    drawdown = running_max - cum_pnl
    return drawdown


def compute_perc_loss_from_high_water_mark(log_rets: pd.Series) -> pd.Series:
    """
    Calculate drawdown in terms of percentage loss.

    :param log_rets: time series of log returns
    :return: drawdown time series as percentage loss
    """
    dd = compute_drawdown(log_rets)
    return 1 - np.exp(-dd)


def compute_time_under_water(pnl: pd.Series) -> pd.Series:
    """
    Generate time under water series.

    :param pnl: time series of per-period PnL
    :return: series of number of consecutive time points under water
    """
    drawdown = compute_drawdown(pnl)
    underwater_mask = drawdown != 0
    # Cumulatively count number of values in True/False groups.
    # Calculate the start of each underwater series.
    underwater_change = underwater_mask != underwater_mask.shift()
    # Assign each underwater series unique number, repeated inside each series.
    underwater_groups = underwater_change.cumsum()
    # Use `.cumcount()` on each underwater series.
    cumulative_count_groups = underwater_mask.groupby(
        underwater_groups
    ).cumcount()
    cumulative_count_groups += 1
    # Set zero drawdown counts to zero.
    n_timepoints_underwater = underwater_mask * cumulative_count_groups
    return n_timepoints_underwater


def compute_turnover(
    pos: pd.Series, unit: Optional[str] = None, nan_mode: Optional[str] = None
) -> pd.Series:
    """
    Compute turnover for a sequence of positions.

    :param pos: sequence of positions
    :param unit: desired output unit (e.g. 'B', 'W', 'M', etc.)
    :param nan_mode: argument for hdatafr.apply_nan_mode()
    :return: turnover
    """
    hdbg.dassert_isinstance(pos, pd.Series)
    nan_mode = nan_mode or "drop"
    pos = hdatafr.apply_nan_mode(pos, mode=nan_mode)
    numerator = pos.diff().abs()
    denominator = (pos.abs() + pos.shift().abs()) / 2
    if unit:
        numerator = csigproc.resample(numerator, rule=unit).sum()
        denominator = csigproc.resample(denominator, rule=unit).sum()
    turnover = numerator / denominator
    # Raise if we upsample.
    if len(turnover) > len(pos):
        raise ValueError("Upsampling is not allowed.")
    return turnover


def compute_average_holding_period(
    pos: pd.Series, unit: Optional[str] = None, nan_mode: Optional[str] = None
) -> pd.Series:
    """
    Compute average holding period for a sequence of positions.

    :param pos: sequence of positions
    :param unit: desired output unit (e.g. 'B', 'W', 'M', etc.)
    :param nan_mode: argument for hdatafr.apply_nan_mode()
    :return: average holding period in specified units
    """
    unit = unit or "B"
    hdbg.dassert_isinstance(pos, pd.Series)
    # TODO(Paul): Determine how to deal with no `freq`.
    hdbg.dassert(pos.index.freq)
    pos_freq_in_year = hdatafr.infer_sampling_points_per_year(pos)
    unit_freq_in_year = hdatafr.infer_sampling_points_per_year(
        csigproc.resample(pos, rule=unit).sum()
    )
    hdbg.dassert_lte(
        unit_freq_in_year,
        pos_freq_in_year,
        msg=f"Upsampling pos freq={pd.infer_freq(pos.index)} to unit freq={unit} is not allowed",
    )
    nan_mode = nan_mode or "drop"
    pos = hdatafr.apply_nan_mode(pos, mode=nan_mode)
    unit_coef = unit_freq_in_year / pos_freq_in_year
    average_holding_period = (
        pos.abs().mean() / pos.diff().abs().mean()
    ) * unit_coef
    return average_holding_period


# TODO(*): Rename `compute_signed_run_starts()`.
def compute_bet_starts(positions: pd.Series) -> pd.Series:
    """
    Calculate the start of each new bet.

    :param positions: series of long/short positions
    :return: a series with a +1 at the start of each new long bet and a -1 at
        the start of each new short bet; NaNs are ignored
    """
    # Drop NaNs before determining bet starts.
    bet_runs = csigproc.sign_normalize(positions).dropna()
    # Determine start of bets.
    # A new bet starts at position j if and only if
    # - the signed value at `j` is +1 or -1 and
    # - the value at `j - 1` is different from the value at `j`
    is_nonzero = bet_runs != 0
    is_diff = bet_runs.diff() != 0
    bet_starts = bet_runs[is_nonzero & is_diff]
    return bet_starts.reindex(positions.index)


def compute_bet_ends(positions: pd.Series) -> pd.Series:
    """
    Calculate the end of each bet.

    NOTE: This function is not casual (because of our choice of indexing).

    :param positions: as in `compute_bet_starts()`
    :return: as in `compute_bet_starts()`, but with long/short bet indicator at
        the last time of the bet. Note that this is not casual.
    """
    # Calculate bet ends by calculating the bet starts of the reversed series.
    reversed_positions = positions.iloc[::-1]
    reversed_bet_starts = compute_bet_starts(reversed_positions)
    bet_ends = reversed_bet_starts.iloc[::-1]
    return bet_ends


def compute_signed_bet_lengths(
    positions: pd.Series,
) -> pd.Series:
    """
    Calculate lengths of bets (in sampling freq).

    :param positions: series of long/short positions
    :return: signed lengths of bets, i.e., the sign indicates whether the
        length corresponds to a long bet or a short bet. Index corresponds to
        end of bet (not causal).
    """
    bet_runs = csigproc.sign_normalize(positions)
    bet_starts = compute_bet_starts(positions)
    bet_ends = compute_bet_ends(positions)
    # Sanity check indices.
    hdbg.dassert(bet_runs.index.equals(bet_starts.index))
    hdbg.dassert(bet_starts.index.equals(bet_ends.index))
    # Get starts of bets or zero positions runs (zero positions are filled with
    # `NaN`s in `compute_bet_runs`).
    bet_starts_idx = bet_starts.loc[bet_starts != 0].dropna().index
    bet_ends_idx = bet_ends.loc[bet_ends != 0].dropna().index
    # To calculate lengths of bets, we take a running cumulative sum of
    # absolute values so that bet lengths can be calculated by subtracting
    # the value at the beginning of each bet from its value at the end.
    bet_runs_abs_cumsum = bet_runs.abs().cumsum()
    # Align bet starts and ends for vectorized subtraction.
    t0s = bet_runs_abs_cumsum.loc[bet_starts_idx].reset_index(drop=True)
    t1s = bet_runs_abs_cumsum.loc[bet_ends_idx].reset_index(drop=True)
    # Subtract and correct for off-by-one.
    bet_lengths = t1s - t0s + 1
    # Recover bet signs (positive for long, negative for short).
    bet_lengths = bet_lengths * bet_starts.loc[bet_starts_idx].reset_index(
        drop=True
    )
    # Reindex according to the bet ends index.
    bet_length_srs = pd.Series(
        index=bet_ends_idx, data=bet_lengths.values, name=positions.name
    )
    return bet_length_srs


# TODO(Paul): Revisit this function and add more test coverage.
def compute_returns_per_bet(
    positions: pd.Series, log_rets: pd.Series
) -> pd.Series:
    """
    Calculate returns for each bet.

    :param positions: series of long/short positions
    :param log_rets: log returns
    :return: signed returns for each bet, index corresponds to the last date of
        bet
    """
    hdbg.dassert(positions.index.equals(log_rets.index))
    hpandas.dassert_strictly_increasing_index(log_rets)
    bet_ends = compute_bet_ends(positions)
    # Retrieve locations of bet starts and bet ends.
    bet_ends_idx = bet_ends.loc[bet_ends != 0].dropna().index
    pnl_bets = log_rets * positions
    bet_rets_cumsum = pnl_bets.cumsum().ffill()
    # Select rets cumsum for periods when bets end.
    bet_rets_cumsum_ends = bet_rets_cumsum.loc[bet_ends_idx].reset_index(
        drop=True
    )
    # Difference between rets cumsum of bet ends is equal to the rets cumsum
    # for the time between these bet ends i.e. rets cumsum per bet.
    rets_per_bet = bet_rets_cumsum_ends.diff()
    # The 1st element of rets_per_bet equals the 1st one of bet_rets_cumsum_ends
    # because it is the first bet so nothing to subtract from it.
    rets_per_bet[0] = bet_rets_cumsum_ends[0]
    rets_per_bet = pd.Series(
        data=rets_per_bet.values, index=bet_ends_idx, name=log_rets.name
    )
    return rets_per_bet


def compute_annualized_return(srs: pd.Series) -> float:
    """
    Annualize mean return.

    :param srs: series with datetimeindex with `freq`
    :return: annualized return; pct rets if `srs` consists of pct rets,
        log rets if `srs` consists of log rets.
    """
    srs = maybe_resample(srs)
    srs = hdatafr.apply_nan_mode(srs, mode="fill_with_zero")
    ppy = hdatafr.infer_sampling_points_per_year(srs)
    mean_rets = srs.mean()
    annualized_mean_rets = ppy * mean_rets
    annualized_mean_rets = cast(float, annualized_mean_rets)
    return annualized_mean_rets


def compute_annualized_volatility(srs: pd.Series) -> float:
    """
    Annualize sample volatility.

    :param srs: series with datetimeindex with `freq`
    :return: annualized volatility (stdev)
    """
    srs = maybe_resample(srs)
    srs = hdatafr.apply_nan_mode(srs, mode="fill_with_zero")
    ppy = hdatafr.infer_sampling_points_per_year(srs)
    std = srs.std()
    annualized_volatility = np.sqrt(ppy) * std
    annualized_volatility = cast(float, annualized_volatility)
    return annualized_volatility


def maybe_resample(srs: pd.Series) -> pd.Series:
    """
    Return `srs` resampled to "B" `srs.index.freq` if is `None`.

    This is a no-op if `srs.index.freq` is not `None`.
    """
    hdbg.dassert_isinstance(srs.index, pd.DatetimeIndex)
    if srs.index.freq is None:
        _LOG.debug("No `freq` detected; resampling to 'B'.")
        srs = srs.resample("B").sum(min_count=1)
    return srs


def stack_prediction_df(
    df: pd.DataFrame,
    id_col: str,
    close_price_col: str,
    vwap_col: str,
    ret_col: str,
    prediction_col: str,
    ath_start: datetime.time,
    ath_end: datetime.time,
    remove_weekends: bool = True,
) -> pd.DataFrame:
    """
    Process and stack a dataframe of predictions and financial data.

    :param df: dataframe of the following form:
        - DatetimeIndex with `freq`
            - datetimes are end-of-bar datetimes (e.g., so all column values
              are knowable at the timestamp, modulo the computation time
              required for the prediction)
        - Two column levels
            - innermost level consists of the names/ids
            - outermost level has features/market data
    :param id_col: name to use for identifier col in output
    :param close_price_col: col name for bar close price
    :param vwap_col: col name for vwap
    :param ret_col: col name for percentage returns
    :param prediction_col: col name for returns prediction
    :param ath_start: active trading hours start time
    :param ath_end: active trading hours end time
    :param remove_weekends: remove any weekend data iff `True`
    :return: dataframe of the following form:
        - RangeIndex
        - Single column level, with columns for timestamps, ids, market data,
          predictions, etc.
        - Predictions are moved from the end-of-bar semantic to
          beginning-of-bar semantic
        - Epoch and timestamps are for beginning-of-bar rather than end
    """
    # Avoid modifying the input dataframe.
    df = df.copy()
    # TODO(Paul): Make this more robust.
    idx_name = df.columns.names[1]
    if idx_name is None:
        idx_name = "level_1"
    # Reindex according to start time.
    bar_start_ts = compute_bar_start_timestamps(df).rename("start_bar_et_ts")
    df.index = bar_start_ts
    bar_start_ts.index = bar_start_ts
    epoch = compute_epoch(df).squeeze().rename("minute_index")
    # Extract market data (price, return, vwap).
    dfs = []
    dfs.append(
        df[[close_price_col]].rename(columns={close_price_col: "eob_close"})
    )
    dfs.append(df[[vwap_col]].rename(columns={vwap_col: "eob_vwap"}))
    dfs.append(df[[ret_col]].rename(columns={ret_col: "eob_ret"}))
    # Perform time shifts for previous-bar price and move prediction to
    # begining-of-bar semantic.
    dfs.append(
        df[[close_price_col]]
        .shift(1)
        .rename(columns={close_price_col: "eopb_close"})
    )
    dfs.append(
        df[[prediction_col]].shift(1).rename(columns={prediction_col: "alpha"})
    )
    # Consolidate the dataframes.
    out_df = pd.concat(dfs, axis=1)
    # Perform ATH time filtering.
    out_df = out_df.between_time(ath_start, ath_end)
    # Maybe remove weekends.
    if remove_weekends:
        out_df = out_df[out_df.index.day_of_week < 5]
    # TODO: Handle NaNs.
    # Stack data and annotate id column.
    out_df = (
        out_df.stack().reset_index(level=1).rename(columns={idx_name: id_col})
    )
    # Add epoch and bar start timestamps.
    out_df = out_df.join(epoch.to_frame())
    out_df = out_df.join(bar_start_ts.apply(lambda x: x.isoformat()).to_frame())
    # Reset the index.
    return out_df.reset_index(drop=True)
