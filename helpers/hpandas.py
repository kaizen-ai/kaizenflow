"""
Import as:

import helpers.hpandas as hpandas
"""
import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hprint as hprint

_LOG = logging.getLogger(__name__)


_LOG.verb_debug = hprint.install_log_verb_debug(_LOG, verbose=False)


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


def dassert_strictly_increasing_index(
    obj: Union[pd.Index, pd.DataFrame, pd.Series],
    msg: Optional[str] = None,
    *args: Any,
) -> None:
    """
    Ensure that a Pandas object has a strictly increasing index.
    """
    index = _get_index(obj)
    # TODO(gp): Understand why mypy reports:
    #   error: "dassert" gets multiple values for keyword argument "msg"
    hdbg.dassert(index.is_monotonic_increasing, msg=msg, *args)
    hdbg.dassert(index.is_unique, msg=msg, *args)


# TODO(gp): Factor out common code related to extracting the index from several
#  pandas data structures.
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
    index = _get_index(obj)
    # TODO(gp): Understand why mypy reports:
    #   error: "dassert" gets multiple values for keyword argument "msg"
    cond = index.is_monotonic_increasing or index.is_monotonic_decreasing
    hdbg.dassert(cond, msg=msg, *args)
    hdbg.dassert(index.is_unique, msg=msg, *args)


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


# #############################################################################


def resample_index(index: pd.DatetimeIndex, frequency: str) -> pd.DatetimeIndex:
    """
    Resample `DatetimeIndex`.

    :param index: `DatetimeIndex` to resample
    :param frequency: frequency from `pd.date_range()` to resample to
    :return: resampled `DatetimeIndex`
    """
    hdbg.dassert_isinstance(index, pd.DatetimeIndex)
    hdbg.dassert(index.is_unique, msg="Index must have only unique values")
    min_date = index.min()
    max_date = index.max()
    # TODO(gp): Preserve the index name.
    # index_name = index.name
    resampled_index = pd.date_range(
        start=min_date,
        end=max_date,
        freq=frequency,
    )
    if len(resampled_index) > len(index):
        # Downsample.
        _LOG.info(
            "Index length increased by %s = %s - %s",
            len(resampled_index) - len(index),
            len(resampled_index),
            len(index),
        )
    elif len(resampled_index) < len(index):
        # Upsample.
        _LOG.info(
            "Index length decreased by %s = %s - %s",
            len(index) - len(resampled_index),
            len(index),
            len(resampled_index),
        )
    else:
        _LOG.info("Index length=%s has not changed", len(index))
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


def drop_duplicates(
    data: Union[pd.Series, pd.DataFrame],
    *args: Any,
    **kwargs: Any,
) -> Union[pd.Series, pd.DataFrame]:
    """
    Wrapper around `pandas.drop_duplicates()`.

    See the official docs:
    - https://pandas.pydata.org/docs/reference/api/pandas.Series.drop_duplicates.html
    - https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop_duplicates.html

    :return: data without duplicates
    """
    _LOG.debug("args=%s, kwargs=%s", str(args), str(kwargs))
    num_rows_before = data.shape[0]
    data_no_dups = data.drop_duplicates(*args, **kwargs)
    num_rows_after = data_no_dups.shape[0]
    if num_rows_before != num_rows_after:
        _LOG.warning(
            "Removed %s rows",
            hprint.perc(num_rows_before - num_rows_after, num_rows_before),
        )
    return data_no_dups


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


def get_df_signature(df: pd.DataFrame, num_rows: int = 3) -> str:
    """
    Compute a simple signature of a dataframe in string format.

    The signature contains metadata about dataframe size and certain
    amount of rows from start and end of a dataframe. It is used for
    testing purposes.
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    txt: List[str] = []
    txt.append("df.shape=%s" % str(df.shape))
    with pd.option_context(
        "display.max_colwidth", int(1e6), "display.max_columns", None
    ):
        txt.append("df.head=\n%s" % df.head(num_rows))
        txt.append("df.tail=\n%s" % df.tail(num_rows))
    txt = "\n".join(txt)
    return txt


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
    Trim df using values in `ts_col_name` in interval bounded by `start_ts` and
    `end_ts`.

    :param ts_col_name: the name of the column. `None` means index
    :param start_ts, end_ts: boundaries of the desired interval
    :param left_close, right_close: encode what to do with the boundaries of the
        interval
        - E.g., [start_ts, end_ts), or (start_ts, end_ts]
    """
    _LOG.verb_debug(hprint.df_to_short_str("df", df, print_dtypes=True))
    _LOG.debug(
        hprint.to_str("ts_col_name start_ts end_ts left_close right_close")
    )
    if df.empty:
        # If the df is empty there is nothing to trim.
        return df
    if start_ts is not None and end_ts is not None:
        hdateti.dassert_tz_compatible(start_ts, end_ts)
        hdbg.dassert_lte(start_ts, end_ts)
    # Handle the index.
    use_index = False
    if ts_col_name is None:
        # Convert the index into a regular column.
        # TODO(gp): Use binary search if there is an index.
        if df.index.name is None:
            _LOG.debug(
                "The df has no index\n%s", hprint.dataframe_to_str(df.head())
            )
            df.index.name = "index"
        ts_col_name = df.index.name
        df = df.reset_index()
        use_index = True
    # TODO(gp): This is inefficient. Make it faster by binary search, if ordered.
    hdbg.dassert_in(ts_col_name, df.columns)
    # Filter based on start_ts.
    _LOG.debug("Filtering by start_ts=%s", start_ts)
    if start_ts is not None:
        _LOG.verb_debug("start_ts=%s", start_ts)
        # Convert the column into `pd.Timestamp` to compare it to `start_ts`.
        # This is needed to sidestep the comparison hell involving `numpy.datetime64`
        # vs Pandas objects.
        tss = pd.to_datetime(df[ts_col_name])
        hdateti.dassert_tz_compatible(tss.iloc[0], start_ts)
        _LOG.verb_debug("tss=\n%s", hprint.dataframe_to_str(tss))
        if left_close:
            mask = tss >= start_ts
        else:
            mask = tss > start_ts
        _LOG.verb_debug("mask=\n%s", hprint.dataframe_to_str(mask))
        df = df[mask]
    # Filter based on end_ts.
    _LOG.debug("Filtering by end_ts=%s", end_ts)
    if not df.empty:
        if end_ts is not None:
            _LOG.debug("Filtering by start_ts=%s", start_ts)
            _LOG.verb_debug("end_ts=%s", end_ts)
            tss = pd.to_datetime(df[ts_col_name])
            hdateti.dassert_tz_compatible(tss.iloc[0], end_ts)
            _LOG.verb_debug("tss=\n%s", hprint.dataframe_to_str(tss))
            if right_close:
                mask = tss <= end_ts
            else:
                mask = tss < end_ts
            _LOG.verb_debug("mask=\n%s", hprint.dataframe_to_str(mask))
            df = df[mask]
    else:
        # If the df is empty there is nothing to trim.
        pass
    if use_index:
        df = df.set_index(ts_col_name, drop=True)
    return df
