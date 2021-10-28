"""
Import as:

import helpers.hpandas as hhpandas
"""
import logging
from typing import Any, Optional, Union

import pandas as pd

import helpers.dbg as hdbg

_LOG = logging.getLogger(__name__)


def dassert_index_is_datetime(
    df: pd.DataFrame, msg: Optional[str] = None, *args: Any
) -> None:
    """
    Ensure that the dataframe has an index containing datetimes.
    """
    # TODO(gp): Add support also for series.
    hdbg.dassert_isinstance(df, pd.DataFrame, msg, *args)
    hdbg.dassert_isinstance(df.index, pd.DatetimeIndex, msg, *args)


def dassert_strictly_increasing_index(
    obj: Union[pd.Index, pd.DataFrame, pd.Series],
    msg: Optional[str] = None,
    *args: Any,
) -> None:
    """
    Ensure that the dataframe has a strictly increasing index.
    """
    if isinstance(obj, pd.Index):
        index = obj
    else:
        index = obj.index
    # TODO(gp): Understand why mypy reports:
    #   error: "dassert" gets multiple values for keyword argument "msg"
    hdbg.dassert(index.is_monotonic_increasing, msg=msg, *args)  # type: ignore
    hdbg.dassert(index.is_unique, msg=msg, *args)  # type: ignore


# TODO(gp): Factor out common code related to extracting the index from several
#  pandas data structures.
# TODO(gp): Not sure it's used or useful?
def dassert_monotonic_index(
    obj: Union[pd.Index, pd.DataFrame, pd.Series],
    msg: Optional[str] = None,
    *args: Any,
) -> None:
    """
    Ensure that the dataframe has a strictly increasing or decreasing index.
    """
    if isinstance(obj, pd.Index):
        index = obj
    else:
        index = obj.index
    # TODO(gp): Understand why mypy reports:
    #   error: "dassert" gets multiple values for keyword argument "msg"
    cond = index.is_monotonic_increasing or index.is_monotonic_decreasing
    hdbg.dassert(cond, msg=msg, *args)  # type: ignore
    hdbg.dassert(index.is_unique, msg=msg, *args)  # type: ignore


def resample_index(index: pd.DatetimeIndex, frequency: str) -> pd.DatetimeIndex:
    """
    Resample `DatetimeIndex`.

    :param index: `DatetimeIndex` to resample
    :param frequency: frequency from `pd.date_range()` to resample to
    :return: resampled `DatetimeIndex`
    """
    hdbg.dassert_isinstance(index, pd.DatetimeIndex)
    min_date = index.min()
    max_date = index.max()
    resampled_index = pd.date_range(
        start=min_date,
        end=max_date,
        freq=frequency,
    )
    _LOG.info(
        "Index length increased by %s = %s - %s",
        len(index) - len(resampled_index),
        len(resampled_index),
        len(index),
    )
    return resampled_index
