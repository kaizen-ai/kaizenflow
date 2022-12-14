"""
Import as:

import helpers.hpandas as hpandas
"""
import logging
import random
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import s3fs
import seaborn as sns

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hsystem as hsystem

# Avoid the following dependency from other `helpers` modules to prevent import cycles.
# import helpers.hs3 as hs3
# import helpers.hsql as hsql
# import helpers.hunit_test as hunitest


_LOG = logging.getLogger(__name__)
# Enable extra verbose debugging. Do not commit.
_TRACE = False


# #############################################################################


def to_series(df: pd.DataFrame) -> pd.Series:
    """
    Convert a pd.DataFrame with a single column into a pd.Series.

    The problem is that empty df or df with a single row are not
    converted correctly to a pd.Series.
    """
    # See https://stackoverflow.com/questions/33246771
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert_eq(df.shape[1], 1, "df=%s doesn't have a single column", df)
    if df.empty:
        srs = pd.Series()
    elif df.shape[0] > 1:
        srs = df.squeeze()
    else:
        srs = pd.Series(df.iloc[0, 0], index=[df.index.values[0]])
        srs.name = df.index.name
    hdbg.dassert_isinstance(srs, pd.Series)
    return srs


def as_series(data: Union[pd.DataFrame, pd.Series]) -> pd.Series:
    """
    Convert a single-column dataframe to a series or no-op if already a series.
    """
    if isinstance(data, pd.Series):
        return data
    return to_series(data)


def dassert_is_days(
    timedelta: pd.Timedelta, *, min_num_days: Optional[int] = None
) -> None:
    hdbg.dassert(
        (timedelta / pd.Timedelta(days=1)).is_integer(),
        "timedelta='%s' is not an integer number of days",
        timedelta,
    )
    if min_num_days is not None:
        hdbg.dassert_lte(1, timedelta.days)


# #############################################################################


def _get_index(obj: Union[pd.Index, pd.DataFrame, pd.Series]) -> pd.Index:
    """
    Return the index of a Pandas object.
    """
    if isinstance(obj, pd.Index):
        index = obj
    else:
        hdbg.dassert_isinstance(obj, (pd.Series, pd.DataFrame))
        index = obj.index
    return index


# TODO(gp): Maybe for symmetry with the other functions, rename to
#  dassert_datetime_index
def dassert_index_is_datetime(
    obj: Union[pd.Index, pd.DataFrame, pd.Series],
    msg: Optional[str] = None,
    *args: Any,
) -> None:
    """
    Ensure that the dataframe has an index containing datetimes.
    """
    index = _get_index(obj)
    hdbg.dassert_isinstance(index, pd.DatetimeIndex, msg, *args)


def dassert_unique_index(
    obj: Union[pd.Index, pd.DataFrame, pd.Series],
    msg: Optional[str] = None,
    *args: Any,
) -> None:
    """
    Ensure that a Pandas object has a unique index.
    """
    index = _get_index(obj)
    if not index.is_unique:
        dup_indices = index.duplicated(keep=False)
        df_dup = obj[dup_indices]
        dup_msg = f"Duplicated rows are:\n{df_to_str(df_dup)}\n"
        if msg is None:
            msg = dup_msg
        else:
            msg = dup_msg + msg
        hdbg.dassert(index.is_unique, msg=msg, *args)


# TODO(gp): @all Add unit tests.
def dassert_increasing_index(
    obj: Union[pd.Index, pd.DataFrame, pd.Series],
    msg: Optional[str] = None,
    *args: Any,
) -> None:
    """
    Ensure that a Pandas object has an increasing index.
    """
    index = _get_index(obj)
    if not index.is_monotonic_increasing:
        # Print information about the problematic indices like:
        # ```
        # Not increasing indices are:
        #                                  full_symbol         open         high
        # timestamp
        # 2018-08-17 01:39:00+00:00  binance::BTC_USDT  6339.250000  6348.910000
        # 2018-08-17 00:01:00+00:00   kucoin::ETH_USDT   286.712987   286.712987
        # ```
        # Find the problematic indices.
        mask = np.diff(index) <= pd.Timedelta(seconds=0)
        mask = np.insert(mask, 0, False)
        # TODO(gp): We might want to specify an integer with how many rows before
        #  after we want to show.
        # Shift back to get the previous index that was creating the issue.
        mask_shift = np.empty_like(mask)
        mask_shift[: len(mask) - 1] = mask[1 : len(mask)]
        mask_shift[len(mask) - 1] = False
        #
        mask = mask | mask_shift
        dup_msg = f"Not increasing indices are:\n{df_to_str(obj[mask])}\n"
        if msg is None:
            msg = dup_msg
        else:
            msg = dup_msg + msg
        # Dump the data to file for further inspection.
        # obj.to_csv("index.csv")
        hdbg.dassert(index.is_monotonic_increasing, msg=msg, *args)


# TODO(gp): @all Add more info in case of failures and unit tests.
def dassert_strictly_increasing_index(
    obj: Union[pd.Index, pd.DataFrame, pd.Series],
    msg: Optional[str] = None,
    *args: Any,
) -> None:
    """
    Ensure that a Pandas object has a strictly increasing index.
    """
    dassert_unique_index(obj, msg, *args)
    dassert_increasing_index(obj, msg, *args)


# TODO(gp): Not sure it's used or useful?
def dassert_monotonic_index(
    obj: Union[pd.Index, pd.DataFrame, pd.Series],
    msg: Optional[str] = None,
    *args: Any,
) -> None:
    """
    Ensure that a Pandas object has a monotonic (i.e., strictly increasing or
    decreasing index).
    """
    dassert_unique_index(obj, msg, *args)
    index = _get_index(obj)
    cond = index.is_monotonic_increasing or index.is_monotonic_decreasing
    hdbg.dassert(cond, msg=msg, *args)


# TODO(Paul): @gp -> dassert_datetime_indexed_df
def dassert_time_indexed_df(
    df: pd.DataFrame, allow_empty: bool, strictly_increasing: bool
) -> None:
    """
    Validate that input dataframe is time indexed and well-formed.

    :param df: dataframe to validate
    :param allow_empty: allow empty data frames
    :param strictly_increasing: if True the index needs to be strictly increasing,
      instead of just increasing
    """
    # Verify that Pandas dataframe is passed as input.
    hdbg.dassert_isinstance(df, pd.DataFrame)
    if not allow_empty:
        # Verify that a non-empty dataframe is passed as input.
        hdbg.dassert_lt(0, df.shape[0])
        # Verify that the dataframe has at least 1 column.
        hdbg.dassert_lte(1, len(df.columns))
    # Verify that the index is increasing.
    if strictly_increasing:
        dassert_strictly_increasing_index(df)
    else:
        dassert_increasing_index(df)
    # Check that the index is in datetime format.
    dassert_index_is_datetime(df)
    # Check that the passed timestamp has timezone info.
    hdateti.dassert_has_tz(df.index[0])


def dassert_valid_remap(to_remap: List[str], remap_dict: Dict[str, str]) -> None:
    """
    Ensure that remapping rows / columns is valid.
    """
    hdbg.dassert_isinstance(to_remap, list)
    hdbg.dassert_isinstance(remap_dict, dict)
    # All the rows / columns to remap, should exist.
    hdbg.dassert_is_subset(
        remap_dict.keys(),
        to_remap,
        "Keys to remap should be a subset of existing columns",
    )
    # The mapping is invertible.
    hdbg.dassert_no_duplicates(remap_dict.keys())
    hdbg.dassert_no_duplicates(remap_dict.values())
    # Rows / columns should not be remapped on existing rows / columns.
    hdbg.dassert_not_intersection(remap_dict.values(), to_remap)


def dassert_series_type_is(
    srs: pd.Series,
    type_: type,
    msg: Optional[str] = None,
    *args: Any,
) -> None:
    """
    Ensure that the data type of `srs` is `type_`.

    Examples of valid series types are
      - np.float64
      - np.int64
      - pd.Timestamp
    """
    hdbg.dassert_isinstance(srs, pd.Series)
    hdbg.dassert_isinstance(type_, type)
    hdbg.dassert_eq(srs.dtype.type, type_, msg, *args)


def dassert_series_type_in(
    srs: pd.Series,
    types: List[type],
    msg: Optional[str] = None,
    *args: Any,
) -> None:
    """
    Ensure that the data type of `srs` is one of the types in `types`.
    """
    hdbg.dassert_isinstance(srs, pd.Series)
    hdbg.dassert_container_type(types, list, type)
    hdbg.dassert_in(srs.dtype.type, types, msg, *args)


def dassert_indices_equal(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    *,
    allow_series: bool = False,
) -> None:
    """
    Ensure that `df1` and `df2` share a common index.

    Print the symmetric difference of indices if equality does not hold.
    """
    if allow_series:
        if isinstance(df1, pd.Series):
            df1 = df1.to_frame()
        if isinstance(df2, pd.Series):
            df2 = df2.to_frame()
    hdbg.dassert_isinstance(df1, pd.DataFrame)
    hdbg.dassert_isinstance(df2, pd.DataFrame)
    hdbg.dassert(
        df1.index.equals(df2.index),
        "df1.index.difference(df2.index)=\n%s\ndf2.index.difference(df1.index)=\n%s",
        df1.index.difference(df2.index),
        df2.index.difference(df1.index),
    )


def dassert_columns_equal(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    *,
    sort_cols: bool = False,
) -> None:
    """
    Ensure that `df1` and `df2` have the same columns.

    Print the symmetric difference of columns if equality does not hold.
    """
    hdbg.dassert_isinstance(df1, pd.DataFrame)
    hdbg.dassert_isinstance(df2, pd.DataFrame)
    if sort_cols:
        _LOG.debug("Sorting dataframe columns.")
        df1 = df1.sort_index(axis=1)
        df2 = df2.sort_index(axis=1)
    hdbg.dassert(
        df1.columns.equals(df2.columns),
        "df1.columns.difference(df2.columns)=\n%s\ndf2.columns.difference(df1.columns)=\n%s",
        df1.columns.difference(df2.columns),
        df2.columns.difference(df1.columns),
    )


def dassert_axes_equal(
    df1: pd.DataFrame, df2: pd.DataFrame, *, sort_cols: bool = False
) -> None:
    """
    Ensure that `df1` and `df2` have the same index and same columns.
    """
    dassert_indices_equal(df1, df2)
    dassert_columns_equal(df1, df2, sort_cols=sort_cols)


# #############################################################################


def resample_index(index: pd.DatetimeIndex, frequency: str) -> pd.DatetimeIndex:
    """
    Resample `DatetimeIndex`.

    :param index: `DatetimeIndex` to resample
    :param frequency: frequency from `pd.date_range()` to resample to
    :return: resampled `DatetimeIndex`
    """
    _LOG.debug(hprint.to_str("index frequency"))
    hdbg.dassert_isinstance(index, pd.DatetimeIndex)
    dassert_unique_index(index, msg="Index must have only unique values")
    min_date = index.min()
    max_date = index.max()
    _LOG.debug("min_date=%s max_date=%s", min_date, max_date)
    # TODO(gp): Preserve the index name.
    # index_name = index.name
    resampled_index = pd.date_range(
        start=min_date,
        end=max_date,
        freq=frequency,
    )
    # Enable detailed debugging.
    if False:
        if len(resampled_index) > len(index):
            # Downsample.
            _LOG.debug(
                "Index length increased by %s = %s - %s",
                len(resampled_index) - len(index),
                len(resampled_index),
                len(index),
            )
        elif len(resampled_index) < len(index):
            # Upsample.
            _LOG.debug(
                "Index length decreased by %s = %s - %s",
                len(index) - len(resampled_index),
                len(index),
                len(resampled_index),
            )
        else:
            _LOG.debug("Index length=%s has not changed", len(index))
    # resampled_index.name = index_name
    return resampled_index


def resample_df(df: pd.DataFrame, frequency: str) -> pd.DataFrame:
    """
    Resample `DataFrame` by placing NaN in missing locations in the index.

    :param df: `DataFrame` to resample
    :param frequency: frequency from `pd.date_range()` to resample to
    :return: resampled `DataFrame`
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    # Preserve the index name.
    index_name = df.index.name
    resampled_index = resample_index(df.index, frequency)
    df_reindex = df.reindex(resampled_index)
    df_reindex.index.name = index_name
    return df_reindex


def find_gaps_in_dataframes(
    df1: pd.DataFrame, df2: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Find data present in one dataframe and missing in the other one.

    :param df1: first dataframe for comparison
    :param df2: second dataframe for comparison
    :return: two dataframes with missing data
    """
    # Get data present in first, but not present in second dataframe.
    first_missing_indices = df2.index.difference(df1.index)
    first_missing_data = df2.loc[first_missing_indices]
    # Get data present in second, but not present in first dataframe.
    second_missing_indices = df1.index.difference(df2.index)
    second_missing_data = df1.loc[second_missing_indices]
    return first_missing_data, second_missing_data


def find_gaps_in_time_series(
    time_series: pd.Series,
    start_timestamp: pd.Timestamp,
    end_timestamp: pd.Timestamp,
    freq: str,
) -> pd.Series:
    """
    Find missing points on a time interval specified by [start_timestamp,
    end_timestamp], where point distribution is determined by <step>.

    If the passed time series is of a unix epoch format. It is
    automatically tranformed to pd.Timestamp.

    :param time_series: time series to find gaps in
    :param start_timestamp: start of the time interval to check
    :param end_timestamp: end of the time interval to check
    :param freq: distance between two data points on the interval.
      Aliases correspond to pandas.date_range's freq parameter,
      i.e. "S" -> second, "T" -> minute.
    :return: pd.Series representing missing points in the source time series.
    """
    _time_series = time_series
    if str(time_series.dtype) in ["int32", "int64"]:
        _time_series = _time_series.map(hdateti.convert_unix_epoch_to_timestamp)
    correct_time_series = pd.date_range(
        start=start_timestamp, end=end_timestamp, freq=freq
    )
    return correct_time_series.difference(_time_series)


def check_and_filter_matching_columns(
    df: pd.DataFrame, required_columns: List[str], filter_data_mode: str
) -> pd.DataFrame:
    """
    Check that columns are the required ones and if not filter data depending
    on `filter_data_mode`.

    :param df: data to check columns for
    :param required_columns: columns to return, skipping columns that are not required
    :param filter_data_mode: control behaviour with respect to extra or missing columns
        - "assert": raise an error if required columns do not match received columns
        - "warn_and_trim": return the intersection of required and received columns and
           issue a warning
    :return: input data as it is if required columns match received columns otherwise
        processed data, see `filter_data_mode`
    """
    received_columns = df.columns.to_list()
    hdbg.dassert_lte(1, len(received_columns))
    #
    if filter_data_mode == "assert":
        # Raise an assertion.
        only_warning = False
    elif filter_data_mode == "warn_and_trim":
        # Just issue a warning.
        only_warning = True
        # Get columns intersection while preserving the order of the columns.
        columns_intersection = [
            col_name
            for col_name in required_columns
            if col_name in received_columns
        ]
        hdbg.dassert_lte(1, len(columns_intersection))
        df = df[columns_intersection]
    else:
        raise ValueError(f"Invalid filter_data_mode='{filter_data_mode}'")
    hdbg.dassert_set_eq(
        required_columns,
        received_columns,
        only_warning=only_warning,
        msg="Received columns do not match required columns.",
    )
    return df


def compare_dataframe_rows(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Compare contents of rows with same indices.

    Index is set to default sequential integer values because compare is
    sensitive to multi index (probably because new multi indexes are created
    for each difference in `compare`). Multi index columns are regular columns now.
    Excess columns are removed so both dataframes are always same shape because
    `compare` expects identical dataframes (same number of rows, columns, etc.).

    :param df1: first dataframe for comparison
    :param df2: second dataframe for comparison
    :return: dataframe with data with same indices and different contents
    """
    # Get rows on which the two dataframe indices match.
    idx_intersection = df1.index.intersection(df2.index)
    # Remove excess columns and reset indexes.
    trimmed_second = df2.loc[idx_intersection].reset_index()
    trimmed_first = df1.loc[idx_intersection].reset_index()
    # Get difference between second and first dataframe.
    data_difference = trimmed_second.compare(trimmed_first)
    # Update data difference with original dataframe index names
    # for easier identification.
    index_names = tuple(df2.index.names)
    # If index or multi index is named, it will be visible in data difference.
    if index_names != (None,):
        for index in data_difference.index:
            for column in index_names:
                data_difference.loc[index, column] = trimmed_second.loc[index][
                    column
                ]
        data_difference = data_difference.convert_dtypes()
    return data_difference


def drop_duplicates(
    data: Union[pd.Series, pd.DataFrame],
    use_index: bool,
    subset: Optional[List[str]] = None,
    *args: Any,
    **kwargs: Any,
) -> Union[pd.Series, pd.DataFrame]:
    """
    Wrap `pandas.drop_duplicates()`.

    See the official docs:
    - https://pandas.pydata.org/docs/reference/api/pandas.Series.drop_duplicates.html
    - https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop_duplicates.html

    :param use_index: use index values for identifying duplicates
    :param subset: a list of columns to consider for identifying duplicates
    :return: data without duplicates
    """
    _LOG.debug(
        "use_index = % s, subset = % s args = % s, kwargs = % s",
        str(use_index),
        str(subset),
        str(args),
        str(kwargs),
    )
    # TODO(Nina): Consider the case when one of the columns has "index" as its name.
    hdbg.dassert_not_in("index", data.columns.tolist())
    num_rows_before = data.shape[0]
    # Get all columns list for subset if no subset is passed.
    if subset is None:
        subset = data.columns.tolist()
    else:
        hdbg.dassert_lte(1, len(subset), "Columns subset cannot be empty")
    if use_index:
        # Save index column name in order to set it back after removing duplicates.
        index_col_name = data.index.name or "index"
        # Add index column to subset columns in order to drop duplicates by it as well.
        subset.insert(0, index_col_name)
        data = data.reset_index()
    #
    data_no_dups = data.drop_duplicates(subset=subset, *args, **kwargs)
    #
    if use_index:
        # Set the index back.
        data_no_dups = data_no_dups.set_index(index_col_name, drop=True)
        # Remove the index's name if the original index does not have one.
        if index_col_name == "index":
            data_no_dups.index.name = None
    # Report the change.
    num_rows_after = data_no_dups.shape[0]
    if num_rows_before != num_rows_after:
        _LOG.debug(
            "Removed %s rows",
            hprint.perc(num_rows_before - num_rows_after, num_rows_before),
        )
    return data_no_dups


def dropna(
    df: pd.DataFrame,
    drop_infs: bool = False,
    report_stats: bool = False,
    *args: Any,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Create a wrapper around pd.dropna() reporting information about the removed
    rows.

    :param df: dataframe to process
    :param drop_infs: if +/- np.inf should be considered as nans
    :param report_stats: if processing stats should be reported
    :return: dataframe with nans dropped
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    num_rows_before = df.shape[0]
    if drop_infs:
        df = df.replace([np.inf, -np.inf], np.nan)
        num_rows_after = df.shape[0]
        pct_removed = hprint.perc(
            num_rows_before - num_rows_after, num_rows_before
        )
        _LOG.info("removed rows with infs: %s", pct_removed)
    df = df.dropna(*args, **kwargs)
    if report_stats:
        num_rows_after = df.shape[0]
        pct_removed = hprint.perc(
            num_rows_before - num_rows_after, num_rows_before
        )
        _LOG.info("removed rows with nans: %s", pct_removed)
    return df


def drop_axis_with_all_nans(
    df: pd.DataFrame,
    drop_rows: bool = True,
    drop_columns: bool = False,
    drop_infs: bool = False,
    report_stats: bool = False,
) -> pd.DataFrame:
    """
    Remove columns and rows not containing information (e.g., with only nans).

    The operation is not performed in place and the resulting df is returned.
    Assume that the index is timestamps.

    :param df: dataframe to process
    :param drop_rows: remove rows with only nans
    :param drop_columns: remove columns with only nans
    :param drop_infs: remove also +/- np.inf
    :param report_stats: report the stats of the operations
    :return: dataframe with specific nan axis dropped
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    if drop_infs:
        df = df.replace([np.inf, -np.inf], np.nan)
    if drop_columns:
        # Remove columns with all nans, if any.
        cols_before = df.columns[:]
        df = df.dropna(axis=1, how="all")
        if report_stats:
            # Report results.
            cols_after = df.columns[:]
            removed_cols = set(cols_before).difference(set(cols_after))
            pct_removed = hprint.perc(
                len(cols_before) - len(cols_after), len(cols_after)
            )
            _LOG.info(
                "removed cols with all nans: %s %s",
                pct_removed,
                hprint.list_to_str(removed_cols),
            )
    if drop_rows:
        # Remove rows with all nans, if any.
        rows_before = df.index[:]
        df = df.dropna(axis=0, how="all")
        if report_stats:
            # Report results.
            rows_after = df.index[:]
            removed_rows = set(rows_before).difference(set(rows_after))
            if len(rows_before) == len(rows_after):
                # Nothing was removed.
                min_ts = max_ts = None
            else:
                # TODO(gp): Report as intervals of dates.
                min_ts = min(removed_rows)
                max_ts = max(removed_rows)
            pct_removed = hprint.perc(
                len(rows_before) - len(rows_after), len(rows_after)
            )
            _LOG.info(
                "removed rows with all nans: %s [%s, %s]",
                pct_removed,
                min_ts,
                max_ts,
            )
    return df


def reindex_on_unix_epoch(
    df: pd.DataFrame, in_col_name: str, unit: str = "s"
) -> pd.DataFrame:
    """
    Transform the column `in_col_name` into a datetime index. `in_col_name`
    contains Unix epoch (e.g., 1638194400) and it is converted into a UTC time.

    :param df: dataframe with a unix epoch
    :param in_col_name: column containing unix epoch
    :param unit: the unit of unix epoch
    """
    # Convert.
    temp_col_name = in_col_name + "_tmp"
    hdbg.dassert_in(in_col_name, df.columns)
    hdbg.dassert_not_in(temp_col_name, df.columns)
    # Save.
    df[temp_col_name] = pd.to_datetime(df[in_col_name], unit=unit, utc=True)
    df.set_index(temp_col_name, inplace=True, drop=True)
    df.index.name = None
    return df


def get_df_signature(df: pd.DataFrame, num_rows: int = 6) -> str:
    """
    Compute a simple signature of a dataframe in string format.

    The signature contains metadata about dataframe size and certain
    amount of rows from start and end of a dataframe. It is used for
    testing purposes.
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    text: List[str] = [f"df.shape={str(df.shape)}"]
    with pd.option_context(
        "display.max_colwidth", int(1e6), "display.max_columns", None
    ):
        # If dataframe size exceeds number of rows, show only subset in form of
        # first and last rows. Otherwise, whole dataframe is shown.
        if len(df) > num_rows:
            text.append(f"df.head=\n{df.head(num_rows // 2)}")
            text.append(f"df.tail=\n{df.tail(num_rows // 2)}")
        else:
            text.append(f"df.full=\n{df}")
    text: str = "\n".join(text)
    return text


# #############################################################################


def trim_df(
    df: pd.DataFrame,
    ts_col_name: Optional[str],
    start_ts: Optional[pd.Timestamp],
    end_ts: Optional[pd.Timestamp],
    left_close: bool,
    right_close: bool,
) -> pd.DataFrame:
    """
    Trim the dataframe using values in `ts_col_name`.

    The dataframe is trimmed in the interval bounded by `start_ts` and `end_ts`.

    :param df: the dataframe to trim
    :param ts_col_name: the name of the column; `None` means index
    :param start_ts: the start boundary for trimming
    :param end_ts: the end boundary for trimming
    :param left_close: whether to include the start boundary of the interval
        - True: [start_ts, ...
        - False: (start_ts, ...
    :param right_close: whether to include the end boundary of the interval
        - True: ..., end_ts]
        - False: ..., end_ts)
    :return: the trimmed dataframe
    """
    if _TRACE:
        _LOG.trace(
            df_to_str(df, print_dtypes=True, print_shape_info=True, tag="df")
        )
    _LOG.debug(
        hprint.to_str("ts_col_name start_ts end_ts left_close right_close")
    )
    if _TRACE:
        _LOG.trace("df=\n%s", df_to_str(df))
    if df.empty:
        # If the df is empty, there is nothing to trim.
        return df
    if start_ts is None and end_ts is None:
        # If no boundaries are specified, there are no points of reference to trim
        # to.
        return df
    num_rows_before = df.shape[0]
    if start_ts is not None and end_ts is not None:
        # Confirm that the interval boundaries are valid.
        hdateti.dassert_tz_compatible(start_ts, end_ts)
        hdbg.dassert_lte(start_ts, end_ts)
    # Get the values to filter by.
    if ts_col_name is None:
        values_to_filter_by = pd.Series(df.index, index=df.index)
    else:
        hdbg.dassert_in(ts_col_name, df.columns)
        values_to_filter_by = df[ts_col_name]
    if values_to_filter_by.is_monotonic_increasing:
        _LOG.trace("df is monotonic")
        # The values are sorted; using the `pd.Series.searchsorted()` method.
        # Find the index corresponding to the left boundary of the interval.
        if start_ts is not None:
            side = "left" if left_close else "right"
            left_idx = values_to_filter_by.searchsorted(start_ts, side)
        else:
            # There is nothing to filter, so the left index is the first one.
            left_idx = 0
        _LOG.debug(hprint.to_str("start_ts left_idx"))
        # Find the index corresponding to the right boundary of the interval.
        if end_ts is not None:
            side = "right" if right_close else "left"
            right_idx = values_to_filter_by.searchsorted(end_ts, side)
        else:
            # There is nothing to filter, so the right index is None.
            right_idx = df.shape[0]
        _LOG.debug(hprint.to_str("end_ts right_idx"))
        #
        hdbg.dassert_lte(0, left_idx)
        hdbg.dassert_lte(left_idx, right_idx)
        hdbg.dassert_lte(right_idx, df.shape[0])
        _LOG.debug(hprint.to_str("start_ts left_idx"))
        if right_idx < df.shape[0]:
            _LOG.debug(hprint.to_str("end_ts right_idx"))
        df = df.iloc[left_idx:right_idx]
    else:
        _LOG.trace("df is not monotonic")
        # The values are not sorted; using the `pd.Series.between` method.
        if left_close and right_close:
            inclusive = "both"
        elif left_close:
            inclusive = "left"
        elif right_close:
            inclusive = "right"
        else:
            inclusive = "neither"
        epsilon = pd.DateOffset(minutes=1)
        if start_ts is None:
            start_ts = values_to_filter_by.min() - epsilon
        if end_ts is None:
            end_ts = values_to_filter_by.max() + epsilon
        df = df[
            values_to_filter_by.between(start_ts, end_ts, inclusive=inclusive)
        ]
    # Report the changes.
    num_rows_after = df.shape[0]
    if num_rows_before != num_rows_after:
        _LOG.debug(
            "Removed %s rows",
            hprint.perc(num_rows_before - num_rows_after, num_rows_before),
        )
    return df


# TODO(Nina): Add `filter_data_mode`.
def merge_dfs(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    threshold_col_name: str,
    *,
    threshold: float = 0.9,
    **pd_merge_kwargs: Any,
) -> pd.DataFrame:
    """
    Wrap `pd.merge`.

    :param threshold_col_name: a column's name to check the minimum overlap on
    :param threshold: minimum overlap of unique values in a specified column to
        perform the merge
    """
    # Sanity check column types.
    threshold_col1 = df1[threshold_col_name]
    threshold_col2 = df2[threshold_col_name]
    only_first_elem = False
    hdbg.dassert_array_has_same_type_element(
        threshold_col1, threshold_col2, only_first_elem
    )
    # TODO(Grisha): @Dan Implement asserts for each asset id.
    # Check that an overlap of unique values is above the specified threshold.
    threshold_unique_values1 = set(threshold_col1)
    threshold_unique_values2 = set(threshold_col2)
    threshold_common_values = set(threshold_unique_values1) & set(
        threshold_unique_values2
    )
    threshold_common_values_share1 = len(threshold_common_values) / len(
        threshold_unique_values1
    )
    threshold_common_values_share2 = len(threshold_common_values) / len(
        threshold_unique_values2
    )
    hdbg.dassert_lte(threshold, threshold_common_values_share1)
    hdbg.dassert_lte(threshold, threshold_common_values_share2)
    # Check that there are no common columns.
    df1_cols = set(df1.columns.to_list()) - set(pd_merge_kwargs["on"])
    df2_cols = set(df2.columns.to_list()) - set(pd_merge_kwargs["on"])
    hdbg.dassert_not_intersection(df1_cols, df2_cols)
    #
    res_df = df1.merge(df2, **pd_merge_kwargs)
    return res_df


# TODO(gp): Is this (ironically) a duplicate of drop_duplicates?
def drop_duplicated(
    df: pd.DataFrame, *, subset: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Implement `df.duplicated` but considering also the index and ignoring nans.
    """
    _LOG.debug("before df=\n%s", df_to_str(df))
    # Move the index to the df.
    old_index_name = df.index.name
    new_index_name = "_index.tmp"
    hdbg.dassert_not_in(new_index_name, df.columns)
    df.index.name = new_index_name
    df.reset_index(drop=False, inplace=True)
    # Remove duplicates by ignoring nans.
    if subset is not None:
        hdbg.dassert_isinstance(subset, list)
        subset = [new_index_name] + subset
    duplicated = df.fillna(0.0).duplicated(subset=subset, keep="first")
    # Report the result of the operation.
    if duplicated.sum() > 0:
        num_rows_before = df.shape[0]
        _LOG.debug("Removing duplicates df=\n%s", df_to_str(df.loc[duplicated]))
        df = df.loc[~duplicated]
        num_rows_after = df.shape[0]
        _LOG.warning(
            "Removed repeated rows num_rows=%s",
            hprint.perc(num_rows_before - num_rows_after, num_rows_before),
        )
    _LOG.debug("after removing duplicates df=\n%s", df_to_str(df))
    # Set the index back.
    df.set_index(new_index_name, inplace=True)
    df.index.name = old_index_name
    _LOG.debug("after df=\n%s", df_to_str(df))
    return df


# #############################################################################


def _display(log_level: int, df: pd.DataFrame) -> None:
    """
    Display a df in a notebook at the given log level.

    The behavior is similar to a command like `_LOG.log(log_level, ...)` but
    for a notebook `display` command.

    :param log_level: log level at which to display a df. E.g., if `log_level =
        logging.DEBUG`, then we display the df only if we are running with
        `-v DEBUG`. If `log_level = logging.INFO` then we don't display it
    """
    from IPython.display import display

    if hsystem.is_running_in_ipynb() and log_level >= hdbg.get_logger_verbosity():
        display(df)


def _df_to_str(
    df: pd.DataFrame,
    num_rows: Optional[int],
    max_columns: int,
    max_colwidth: int,
    max_rows: int,
    precision: int,
    display_width: int,
    use_tabulate: bool,
    log_level: int,
) -> str:
    is_in_ipynb = hsystem.is_running_in_ipynb()
    out = []
    # Set dataframe print options.
    with pd.option_context(
        "display.max_colwidth",
        max_colwidth,
        # "display.height", 1000,
        "display.max_rows",
        max_rows,
        "display.precision",
        precision,
        "display.max_columns",
        max_columns,
        "display.width",
        display_width,
    ):
        if use_tabulate:
            import tabulate

            out.append(tabulate.tabulate(df, headers="keys", tablefmt="psql"))
        if num_rows is None or df.shape[0] <= num_rows:
            # Print the entire data frame.
            if not is_in_ipynb:
                out.append(str(df))
            else:
                # Display dataframe.
                _display(log_level, df)
        else:
            nr = num_rows // 2
            if not is_in_ipynb:
                # Print top and bottom of df.
                out.append(str(df.head(nr)))
                out.append("...")
                tail_str = str(df.tail(nr))
                # Remove index and columns from tail_df.
                skipped_rows = 1
                if df.index.name:
                    skipped_rows += 1
                tail_str = "\n".join(tail_str.split("\n")[skipped_rows:])
                out.append(tail_str)
            else:
                # TODO(gp): @all use this approach also above and update all the
                #  unit tests.
                df = [
                    df.head(nr),
                    pd.DataFrame(
                        [["..."] * df.shape[1]], index=[" "], columns=df.columns
                    ),
                    df.tail(nr),
                ]
                df = pd.concat(df)
                # Display dataframe.
                _display(log_level, df)
    if not is_in_ipynb:
        txt = "\n".join(out)
    else:
        txt = ""
    return txt


# TODO(gp): Maybe we can have a `_LOG_df_to_str(log_level, *args, **kwargs)` that
#  calls `_LOG.log(log_level, hpandas.df_to_str(*args, **kwargs, log_level=log_level))`.
# TODO(gp): We should make sure this works properly in a notebook, although
#  it's not easy to unit test.
def df_to_str(
    df: Union[pd.DataFrame, pd.Series, pd.Index],
    *,
    num_rows: Optional[int] = 6,
    print_dtypes: bool = False,
    print_shape_info: bool = False,
    print_nan_info: bool = False,
    print_memory_usage: bool = False,
    memory_usage_mode: str = "human_readable",
    tag: Optional[str] = None,
    max_columns: int = 10000,
    max_colwidth: int = 2000,
    max_rows: int = 500,
    precision: int = 6,
    display_width: int = 10000,
    use_tabulate: bool = False,
    log_level: int = logging.DEBUG,
) -> str:
    """
    Print a dataframe to string reporting all the columns without trimming.

    Note that code like: `_LOG.info(hpandas.df_to_str(df, num_rows=3))` works
    properly when called from outside a notebook, i.e., the dataframe is printed
    But it won't display the dataframe in a notebook, since the default level at
    which the dataframe is displayed is `logging.DEBUG`.

    In this case to get the correct behavior one should do:

    ```
    log_level = ...
    _LOG.log(log_level, hpandas.df_to_str(df, num_rows=3, log_level=log_level))
    ```

    :param: num_rows: max number of rows to print (half from the top and half from
        the bottom of the dataframe)
        - `None` to print the entire dataframe
    :param print_dtypes: report dataframe types and information about the type of
        each column by looking at the first value
    :param print_shape_info: report dataframe shape, index and columns
    :param print_memory_usage: report memory use for each
    """
    if df is None:
        return ""
    if isinstance(df, pd.Series):
        df = pd.DataFrame(df)
    elif isinstance(df, pd.Index):
        df = df.to_frame(index=False)
    hdbg.dassert_isinstance(df, pd.DataFrame)
    out = []
    # Print the tag.
    if tag is not None:
        out.append(f"# {tag}=")
    if not df.empty:
        # Print information about the shape and index.
        # TODO(Nikola): Revisit and rename print_shape_info to print_axes_info
        if print_shape_info:
            # TODO(gp): Unfortunately we can't improve this part of the output
            # since there are many golden inside the code that would need to be
            # updated. Consider automating updating the expected values in the code.
            txt = f"index=[{df.index.min()}, {df.index.max()}]"
            out.append(txt)
            txt = f"columns={','.join(map(str, df.columns))}"
            out.append(txt)
            txt = f"shape={str(df.shape)}"
            out.append(txt)
        # Print information about the types.
        if print_dtypes:
            out.append("* type=")

            table = []

            def _report_srs_stats(srs: pd.Series) -> List[Any]:
                """
                Report dtype, the first element, and its type of series.
                """
                row: List[Any] = []
                first_elem = srs.values[0]
                num_unique = srs.nunique()
                num_nans = srs.isna().sum()
                row.extend(
                    [
                        srs.dtype,
                        hprint.perc(num_unique, len(srs)),
                        hprint.perc(num_nans, len(srs)),
                        first_elem,
                        type(first_elem),
                    ]
                )
                return row

            row = []
            col_name = "index"
            row.append(col_name)
            row.extend(_report_srs_stats(df.index))
            row = map(str, row)
            table.append(row)
            for col_name in df.columns:
                row_: List[Any] = []
                row_.append(col_name)
                row_.extend(_report_srs_stats(df[col_name]))
                row_ = map(str, row_)
                table.append(row_)
            #
            columns = [
                "col_name",
                "dtype",
                "num_unique",
                "num_nans",
                "first_elem",
                "type(first_elem)",
            ]
            df_stats = pd.DataFrame(table, columns=columns)
            stats_num_rows = None
            df_stats_as_str = _df_to_str(
                df_stats,
                stats_num_rows,
                max_columns,
                max_colwidth,
                max_rows,
                precision,
                display_width,
                use_tabulate,
                log_level,
            )
            out.append(df_stats_as_str)
        # Print info about memory usage.
        if print_memory_usage:
            out.append("* memory=")
            mem_use_df = pd.concat(
                [df.memory_usage(deep=False), df.memory_usage(deep=True)], axis=1
            )
            mem_use_df.columns = ["shallow", "deep"]
            # Add total row.
            mem_use_df_total = mem_use_df.sum(axis=0)
            mem_use_df_total.name = "Total"
            mem_use_df = mem_use_df.append(mem_use_df_total.T)
            # Convert into the desired format.
            if memory_usage_mode == "bytes":
                pass
            elif memory_usage_mode == "human_readable":
                import helpers.hintrospection as hintros

                mem_use_df = mem_use_df.applymap(hintros.format_size)
            else:
                raise ValueError(
                    f"Invalid memory_usage_mode='{memory_usage_mode}'"
                )
            memory_num_rows = None
            memory_usage_as_txt = _df_to_str(
                mem_use_df,
                memory_num_rows,
                max_columns,
                max_colwidth,
                max_rows,
                precision,
                display_width,
                use_tabulate,
                log_level,
            )
            out.append(memory_usage_as_txt)
        # Print info about nans.
        if print_nan_info:
            num_elems = df.shape[0] * df.shape[1]
            num_nans = df.isna().sum().sum()
            txt = f"num_nans={hprint.perc(num_nans, num_elems)}"
            out.append(txt)
            #
            num_zeros = df.isnull().sum().sum()
            txt = f"num_zeros={hprint.perc(num_zeros, num_elems)}"
            out.append(txt)
            # TODO(gp): np can't do isinf on objects like strings.
            # num_infinite = np.isinf(df).sum().sum()
            # txt = "num_infinite=%s" % hprint.perc(num_infinite, num_elems)
            # out.append(txt)
            #
            num_nan_rows = df.dropna().shape[0]
            txt = f"num_nan_rows={hprint.perc(num_nan_rows, num_elems)}"
            out.append(txt)
            #
            num_nan_cols = df.dropna(axis=1).shape[1]
            txt = f"num_nan_cols={hprint.perc(num_nan_cols, num_elems)}"
            out.append(txt)
    if hsystem.is_running_in_ipynb():
        if len(out) > 0 and log_level >= hdbg.get_logger_verbosity():
            print("\n".join(out))
        txt = None
    # Print the df.
    df_as_str = _df_to_str(
        df,
        num_rows,
        max_columns,
        max_colwidth,
        max_rows,
        precision,
        display_width,
        use_tabulate,
        log_level,
    )
    if not hsystem.is_running_in_ipynb():
        out.append(df_as_str)
        txt = "\n".join(out)
    return txt


def convert_df_to_json_string(
    df: pd.DataFrame,
    n_head: Optional[int] = 10,
    n_tail: Optional[int] = 10,
    columns_order: Optional[List[str]] = None,
) -> str:
    """
    Convert dataframe to pretty-printed JSON string.

    To select all rows of the dataframe, pass `n_head` as None.

    :param df: dataframe to convert
    :param n_head: number of printed top rows
    :param n_tail: number of printed bottom rows
    :param columns_order: order for the KG columns sort
    :return: dataframe converted to JSON string
    """
    # Append shape of the initial dataframe.
    shape = f"original shape={df.shape}"
    # Reorder columns.
    if columns_order is not None:
        hdbg.dassert_set_eq(columns_order, df.cols)
        df = df[columns_order]
    # Select head.
    if n_head is not None:
        head_df = df.head(n_head)
    else:
        # If no n_head provided, append entire dataframe.
        head_df = df
    # Transform head to json.
    head_json = head_df.to_json(
        orient="index",
        force_ascii=False,
        indent=4,
        default_handler=str,
        date_format="iso",
        date_unit="s",
    )
    if n_tail is not None:
        # Transform tail to json.
        tail = df.tail(n_tail)
        tail_json = tail.to_json(
            orient="index",
            force_ascii=False,
            indent=4,
            default_handler=str,
            date_format="iso",
            date_unit="s",
        )
    else:
        # If no tail specified, append an empty string.
        tail_json = ""
    # Join shape and dataframe to single string.
    output_str = "\n".join([shape, "Head:", head_json, "Tail:", tail_json])
    return output_str


# #############################################################################


def convert_col_to_int(
    df: pd.DataFrame,
    col: str,
) -> pd.DataFrame:
    """
    Convert a column to an integer column.

    Example use case: Parquet uses categoricals. If supplied with a
    categorical-type column, this function will convert it to an integer
    column.
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert_isinstance(col, str)
    hdbg.dassert_in(col, df.columns)
    # Attempt the conversion.
    df[col] = df[col].astype("int64")
    # Trust, but verify.
    dassert_series_type_is(df[col], np.int64)
    return df


# #############################################################################


def read_csv_to_df(
    stream: Union[str, s3fs.core.S3File, s3fs.core.S3FileSystem],
    *args: Any,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Read a CSV file into a `pd.DataFrame`.
    """
    # Gets filename from stream if it is not already a string,
    # so it can be inspected for extension type.
    file_name = stream if isinstance(stream, str) else vars(stream)["path"]
    # Handle zipped files.
    if any(file_name.endswith(ext) for ext in (".gzip", ".gz", ".tgz")):
        hdbg.dassert_not_in("compression", kwargs)
        kwargs["compression"] = "gzip"
    elif file_name.endswith(".zip"):
        hdbg.dassert_not_in("compression", kwargs)
        kwargs["compression"] = "zip"
    # Read.
    _LOG.debug(hprint.to_str("args kwargs"))
    df = pd.read_csv(stream, *args, **kwargs)
    return df


def read_parquet_to_df(
    stream: Union[str, s3fs.core.S3File, s3fs.core.S3FileSystem],
    *args: Any,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Read a Parquet file into a `pd.DataFrame`.
    """
    # Read.
    _LOG.debug(hprint.to_str("args kwargs"))
    df = pd.read_parquet(stream, *args, **kwargs)
    return df


# #############################################################################


# TODO(Paul): Add unit tests.
def compute_weighted_sum(
    dfs: Dict[str, pd.DataFrame],
    weights: pd.DataFrame,
) -> Dict[str, pd.DataFrame]:
    """
    Compute weighted sums of `dfs` using `weights`.

    :param dfs: dataframes keyed by id; all dfs should have the same index
        and cols
    :param weights: float weights indexed by id with unique col names
    :return: weighted sums keyed by weight col names
    """
    hdbg.dassert_isinstance(dfs, dict)
    hdbg.dassert(dfs, "dictionary of dfs must be nonempty")
    # Get a dataframe from the dictionary and record its index and columns.
    id_ = list(dfs)[0]
    hdbg.dassert_isinstance(id_, str)
    df = dfs[id_]
    hdbg.dassert_isinstance(df, pd.DataFrame)
    idx = df.index
    cols = df.columns
    # Sanity-check dataframes in dictionary.
    for key, value in dfs.items():
        hdbg.dassert_isinstance(key, str)
        hdbg.dassert_isinstance(value, pd.DataFrame)
        hdbg.dassert(
            value.index.equals(idx),
            "Index equality fails for keys=%s, %s",
            id_,
            key,
        )
        hdbg.dassert(
            value.columns.equals(cols),
            "Column equality fails for keys=%s, %s",
            id_,
            key,
        )
    # Sanity-check weights.
    hdbg.dassert_isinstance(weights, pd.DataFrame)
    hdbg.dassert_eq(weights.columns.nlevels, 1)
    hdbg.dassert(not weights.columns.has_duplicates)
    hdbg.dassert_set_eq(weights.index.to_list(), list(dfs))
    # Create a multiindexed dataframe to facilitate computing the weighted sums.
    weighted_dfs = {}
    combined_df = pd.concat(dfs.values(), axis=1, keys=dfs.keys())
    # TODO(Paul): Consider relaxing the NaN-handling.
    for col in weights.columns:
        weighted_combined_df = combined_df.multiply(weights[col], level=0)
        weighted_sums = weighted_combined_df.groupby(axis=1, level=1).sum(
            min_count=len(dfs)
        )
        weighted_dfs[col] = weighted_sums
    return weighted_dfs


def subset_df(df: pd.DataFrame, nrows: int, seed: int = 42) -> pd.DataFrame:
    """
    Remove N rows from the input data and shuffle the remaining ones.

    :param df: input data
    :param nrows: the number of rows to remove from the original data
    :param seed: see `random.seed()`
    :return: shuffled data with removed rows
    """
    hdbg.dassert_lte(1, nrows)
    hdbg.dassert_lte(nrows, df.shape[0])
    idx = list(range(df.shape[0]))
    random.seed(seed)
    random.shuffle(idx)
    idx = sorted(idx[nrows:])
    return df.iloc[idx]


def get_random_df(
    num_cols: int,
    seed: Optional[int] = None,
    date_range_kwargs: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Compute df with random data with `num_cols` columns and index obtained by
    calling `pd.date_range(**kwargs)`.

    :param num_cols: the number of columns in a DataFrame to generate
    :param seed: see `random.seed()`
    :param date_range_kwargs: kwargs for `pd.date_range()`
    """
    if seed:
        np.random.seed(seed)
    dt = pd.date_range(**date_range_kwargs)
    df = pd.DataFrame(np.random.rand(len(dt), num_cols), index=dt)
    return df


# #############################################################################

# TODO(gp): -> AxisNameSet
ColumnSet = Optional[Union[str, List[str]]]


# TODO(gp): -> _resolve_axis_names
def _resolve_column_names(
    column_set: ColumnSet,
    columns: Union[List[str], pd.Index],
    *,
    keep_order: bool = False,
) -> List[str]:
    """
    Change format of the columns and perform some sanity checks.

    :param column_set: columns to proceed
    :param columns: all columns available
    :param keep_order: preserve the original order or allow sorting
    """
    # Ensure that `columns` is well-formed.
    if isinstance(columns, pd.Index):
        columns = columns.to_list()
    hdbg.dassert_isinstance(columns, list)
    hdbg.dassert_lte(1, len(columns))
    #
    if column_set is None:
        # Columns were not specified, thus use the list of all the columns.
        column_set = columns
    else:
        if isinstance(column_set, str):
            column_set = [column_set]
        hdbg.dassert_isinstance(column_set, list)
        hdbg.dassert_lte(1, len(column_set))
        hdbg.dassert_is_subset(column_set, columns)
        if keep_order:
            # Keep the selected columns in the same order as in the original
            # `columns`.
            column_set = [c for c in columns if c in column_set]
    return column_set


# TODO(Grisha): finish the function.
# TODO(Grisha): merge with the one in `dataflow.model.correlation.py`?
def remove_outliers(
    df: pd.DataFrame,
    lower_quantile: float,
    *,
    column_set: ColumnSet,
    # TODO(Grisha): the params are not used.
    fill_value: float = np.nan,
    mode: str = "remove_outliers",
    axis: Any = 0,
    upper_quantile: Optional[float] = None,
) -> pd.DataFrame:
    hdbg.dassert_eq(len(df.shape), 2, "Multi-index dfs not supported")
    #
    hdbg.dassert_lte(0.0, lower_quantile)
    if upper_quantile is None:
        upper_quantile = 1.0 - lower_quantile
    hdbg.dassert_lte(lower_quantile, upper_quantile)
    hdbg.dassert_lte(upper_quantile, 1.0)
    #
    df = df.copy()
    if axis == 0:
        all_columns = df.columns
        columns = _resolve_column_names(column_set, all_columns)
        hdbg.dassert_is_subset(columns, df.columns)
        for column in all_columns:
            if column in columns:
                df[column] = df[column].quantile([lower_quantile, upper_quantile])
    elif axis == 1:
        all_rows = df.rows
        rows = _resolve_column_names(column_set, all_rows)
        hdbg.dassert_is_subset(rows, df.rows)
        for row in all_rows:
            if row in rows:
                df[row] = df[row].quantile([lower_quantile, upper_quantile])
    else:
        raise ValueError(f"Invalid axis='{axis}'")
    return df


# #############################################################################


def heatmap_df(df: pd.DataFrame, *, axis: Any = None) -> pd.DataFrame:
    """
    Colorize a df with a heatmap depending on the numeric values.

    :param axis: along which axis to compute the heatmap
        - 0 colorize along rows
        - 1 colorize along columns
        - None colorize
    """
    cm = sns.diverging_palette(5, 250, as_cmap=True)
    df = df.style.background_gradient(axis=axis, cmap=cm)
    return df


def compare_dfs(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    *,
    row_mode: str = "equal",
    column_mode: str = "equal",
    diff_mode: str = "diff",
    assert_diff_threshold: float = 1e-3,
    close_to_zero_threshold: float = 1e-6,
    zero_vs_zero_is_zero: bool = True,
    remove_inf: bool = True,
    log_level: int = logging.DEBUG,
) -> pd.DataFrame:
    """
    Compare two dataframes.

    This works for dataframes with and without multi-index.

    :param row_mode: control how the rows are handled
        - "equal": rows need to be the same for the two dataframes
        - "inner": compute the common rows for the two dataframes
    :param column_mode: same as `row_mode`
    :param diff_mode: control how the dataframes are compared in terms of
        corresponding elements
        - "diff": use the difference
        - "pct_change": use the percentage difference
    :param assert_diff_threshold: maximum allowed total difference
        - do not assert if `None`
        - works when `diff_mode` is "pct_change"
    :param close_to_zero_threshold: round numbers below the threshold to 0
    :param zero_vs_zero_is_zero: replace the diff with 0 when comparing 0 to 0
        if True, otherwise keep the actual result
    :param remove_inf: replace +-inf with `np.nan`
    :param log_level: logging level
    :return: a singe dataframe with differences as values
    """
    # TODO(gp): Factor out this logic and use it for both compare_visually_dfs
    #  and
    if row_mode == "equal":
        hdbg.dassert_eq(list(df1.index), list(df2.index))
    elif row_mode == "inner":
        # TODO(gp): Add sorting on demand, otherwise keep the columns in order.
        same_rows = list((set(df1.index)).intersection(set(df2.index)))
        df1 = df1[df1.index.isin(same_rows)]
        df2 = df2[df2.index.isin(same_rows)]
    else:
        raise ValueError(f"Invalid row_mode='{row_mode}'")
    #
    if column_mode == "equal":
        hdbg.dassert_eq(sorted(df1.columns), sorted(df2.columns))
    elif column_mode == "inner":
        # TODO(gp): Add sorting on demand, otherwise keep the columns in order.
        col_names = sorted(list(set(df1.columns).intersection(set(df2.columns))))
        df1 = df1[col_names]
        df2 = df2[col_names]
    else:
        raise ValueError(f"Invalid column_mode='{column_mode}'")
    close_to_zero_threshold_mask = lambda x: abs(x) < close_to_zero_threshold
    # Round small numbers to 0 to exclude them from the diff computation.
    df1[close_to_zero_threshold_mask] = df1[close_to_zero_threshold_mask].round(0)
    df2[close_to_zero_threshold_mask] = df2[close_to_zero_threshold_mask].round(0)
    # Compute the difference df.
    if diff_mode == "diff":
        df_diff = df1 - df2
    elif diff_mode == "pct_change":
        df_diff = 100 * (df1 - df2) / df2
        if zero_vs_zero_is_zero:
            # When comparing 0 to 0 set the diff (which is NaN by default) to 0.
            df1_mask = df1 == 0
            df2_mask = df2 == 0
            zero_vs_zero_mask =  df1_mask & df2_mask
            df_diff[zero_vs_zero_mask] = 0
    else:
        raise ValueError(f"diff_mode={diff_mode}")
    df_diff = df_diff.add_suffix(f".{diff_mode}")
    if diff_mode == "pct_change" and assert_diff_threshold is not None:
        # TODO(Grisha): generalize for the other modes.
        # Report max diff.
        max_diff = df_diff.abs().max().max()
        _LOG.log(log_level, "Max difference factor: %s", max_diff)
        if assert_diff_threshold is not None:
            hdbg.dassert_lte(assert_diff_threshold, 1.0)
            hdbg.dassert_lte(0.0, assert_diff_threshold)
            # TODO(Grisha): it works only if `remove_inf` is True.
            hdbg.dassert_lte(max_diff, assert_diff_threshold)
    if remove_inf:
        df_diff = df_diff.replace([np.inf, -np.inf], np.nan)
    return df_diff


# #############################################################################
# Multi-index dfs
# #############################################################################


def list_to_str(
    vals: List[Any],
    *,
    sep_char: str = ", ",
    enclose_str_char: str = "'",
    max_num: Optional[int] = 10,
) -> str:
    """
    TODO(gp): Add docstring.
    """
    vals_as_str = list(map(str, vals))
    # Add a str around.
    if enclose_str_char:
        vals_as_str = [
            enclose_str_char + v + enclose_str_char for v in vals_as_str
        ]
    #
    ret = "%s [" % len(vals)
    if max_num is not None and len(vals) > max_num:
        hdbg.dassert_lt(1, max_num)
        ret += sep_char.join(vals_as_str[: int(max_num / 2)])
        ret += sep_char + "..." + sep_char
        ret += sep_char.join(vals_as_str[-int(max_num / 2) :])
    else:
        ret += sep_char.join(vals_as_str)
    ret += "]"
    return ret


def multiindex_df_info(
    df: pd.DataFrame,
    *,
    log_level: int = logging.INFO,
    **list_to_str_kwargs: Dict[str, Any],
) -> str:
    """
    Report information about a multi-index df.
    """
    hdbg.dassert_eq(2, len(df.columns.levels))
    columns_level0 = df.columns.levels[0]
    columns_level1 = df.columns.levels[1]
    rows = df.index
    ret = []
    ret.append(
        "shape=%s x %s x %s"
        % (len(columns_level0), len(columns_level1), len(rows))
    )
    ret.append(
        "columns_level0=%s" % list_to_str(columns_level0, **list_to_str_kwargs)
    )
    ret.append(
        "columns_level1=%s" % list_to_str(columns_level1, **list_to_str_kwargs)
    )
    ret.append("rows=%s" % list_to_str(rows, **list_to_str_kwargs))
    ret = "\n".join(ret)
    _LOG.log(log_level, ret)
    return ret


def subset_multiindex_df(
    df: pd.DataFrame,
    *,
    # TODO(gp): Consider passing trim_df_kwargs as kwargs.
    start_timestamp: Optional[pd.Timestamp] = None,
    end_timestamp: Optional[pd.Timestamp] = None,
    columns_level0: ColumnSet = None,
    columns_level1: ColumnSet = None,
    keep_order: bool = False,
) -> pd.DataFrame:
    """
    Filter multi-index DataFrame by timestamp index and column levels.

    :param start_timestamp: see `trim_df()`
    :param end_timestamp: see `trim_df()`
    :param columns_level0: column names that corresponds to `df.columns.levels[0]`
        - `None` means no filtering
    :param columns_level1: column names that corresponds to `df.columns.levels[1]`
        - `None` means no filtering
    :param keep_order: see `_resolve_column_names()`
    :return: filtered DataFrame
    """
    hdbg.dassert_eq(2, len(df.columns.levels))
    # Filter by timestamp.
    allow_empty = False
    strictly_increasing = False
    dassert_time_indexed_df(df, allow_empty, strictly_increasing)
    df = trim_df(
        df,
        ts_col_name=None,
        start_ts=start_timestamp,
        end_ts=end_timestamp,
        left_close=True,
        right_close=True,
    )
    # Filter level 0.
    all_columns_level0 = df.columns.levels[0]
    columns_level0 = _resolve_column_names(
        columns_level0, all_columns_level0, keep_order=keep_order
    )
    hdbg.dassert_is_subset(columns_level0, df.columns.levels[0])
    df = df[columns_level0]
    # Filter level 1.
    all_columns_level1 = df.columns.levels[1]
    columns_level1 = _resolve_column_names(
        columns_level1, all_columns_level1, keep_order=keep_order
    )
    hdbg.dassert_is_subset(columns_level1, df.columns.levels[1])
    df = df.swaplevel(axis=1)[columns_level1].swaplevel(axis=1)
    return df


# #############################################################################


def compare_multiindex_dfs(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    *,
    subset_multiindex_df_kwargs: Optional[Dict[str, Any]] = None,
    compare_dfs_kwargs: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    - Subset both multi-index dfs, if needed
    - Compare dfs

    :param subset_multiindex_df: params for `subset_multiindex_df()`
    :param compare_dfs_kwargs: params for `compare_dfs()`
    :return: df with differences as values
    """
    # Subset dfs.
    if subset_multiindex_df_kwargs is None:
        subset_multiindex_df_kwargs = {}
    subset_df1 = subset_multiindex_df(df1, **subset_multiindex_df_kwargs)
    subset_df2 = subset_multiindex_df(df2, **subset_multiindex_df_kwargs)
    # Compare dfs.
    if compare_dfs_kwargs is None:
        compare_dfs_kwargs = {}
    diff_df = compare_dfs(subset_df1, subset_df2, **compare_dfs_kwargs)
    return diff_df


# #############################################################################


def compute_duration_df(
    tag_to_df: Dict[str, pd.DataFrame],
    *,
    intersect_dfs: bool = False,
    valid_intersect: bool = False,
) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """
    Compute a df with some statistics about the time index.

    E.g.,
    ```
                   min_index   max_index   min_valid_index   max_valid_index
    tag1
    tag2
    ```

    :param intersect_dfs: return a transformed dict with the intersection of
        indices of all the dfs if True, otherwise return the input data as is
    :param valid_intersect: intersect indices without NaNs if True, otherwise
        intersect indices as is
    :return: timestamp stats and updated dict of dfs, see `intersect_dfs` param
    """
    hdbg.dassert_isinstance(tag_to_df, Dict)
    # Create df and assign columns.
    data_stats = pd.DataFrame()
    min_col = "min_index"
    max_col = "max_index"
    min_valid_index_col = "min_valid_index"
    max_valid_index_col = "max_valid_index"
    # Collect timestamp info from all dfs.
    for tag in tag_to_df.keys():
        # Check that the passed timestamp has timezone info.
        hdateti.dassert_has_tz(tag_to_df[tag].index[0])
        dassert_index_is_datetime(tag_to_df[tag])
        # Compute timestamp stats.
        data_stats.loc[tag, min_col] = tag_to_df[tag].index.min()
        data_stats.loc[tag, max_col] = tag_to_df[tag].index.max()
        data_stats.loc[tag, min_valid_index_col] = (
            tag_to_df[tag].dropna().index.min()
        )
        data_stats.loc[tag, max_valid_index_col] = (
            tag_to_df[tag].dropna().index.max()
        )
    # Make a copy so we do not modify the original data.
    tag_to_df_updated = tag_to_df.copy()
    # Change the initial dfs with intersection.
    if intersect_dfs:
        if valid_intersect:
            # Assign start, end date column according to specs.
            min_col = min_valid_index_col
            max_col = max_valid_index_col
        # The start of the intersection will be the max value amongt all start dates.
        intersection_start_date = data_stats[min_col].max()
        # The end of the intersection will be the min value amongt all end dates.
        intersection_end_date = data_stats[max_col].min()
        for tag in tag_to_df_updated.keys():
            df = trim_df(
                tag_to_df_updated[tag],
                ts_col_name=None,
                start_ts=intersection_start_date,
                end_ts=intersection_end_date,
                left_close=True,
                right_close=True,
            )
            tag_to_df_updated[tag] = df
    return data_stats, tag_to_df_updated
