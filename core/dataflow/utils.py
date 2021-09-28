"""
Types and utilities used in entire core/dataflow.

Note this file should not depend on anything in `core`.

Import as:

import core.dataflow.utils as cdtfu
"""

import datetime
import io
import logging
from typing import Callable, List, Tuple, Union

import pandas as pd

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


# #############################################################################
# Node columns.
# #############################################################################

NodeColumn = Union[int, str]
# A list of columns or a function that returns a list of column types.
NodeColumnList = Union[List[NodeColumn], Callable[[], List[NodeColumn]]]


# #############################################################################
# Intervals.
# #############################################################################


IntervalEndpoint = Union[datetime.datetime, pd.Timestamp, None]
# Intervals are considered as closed, i.e., [a, b]. An endpoint equal to `None` means
# unbounded interval on that direction.
Interval = Tuple[IntervalEndpoint, IntervalEndpoint]
Intervals = List[Interval]


# #############################################################################


# TODO(gp): Move to helpers/printing.py since it's general.
def get_df_info_as_string(
    df: pd.DataFrame, exclude_memory_usage: bool = True
) -> str:
    """
    Get dataframe info as string.

    :param df: dataframe
    :param exclude_memory_usage: whether to exclude memory usage information
    :return: dataframe info as `str`
    """
    buffer = io.StringIO()
    df.info(buf=buffer)
    info = buffer.getvalue()
    if exclude_memory_usage:
        # Remove memory usage (and a newline).
        info = info.rsplit("\n", maxsplit=2)[0]
    return info


# TODO(gp): Maybe move to helpers.pandas_helpers since it's general.
def merge_dataframes(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
) -> pd.DataFrame:
    """
    Safely merges identically indexed `df1` and `df2`.

    This merge function checks that `df1` and `df2`
      - have equal indices
      - have no column duplicates
      - do not share column names
      - have the same column levels

    :return: merge of `df1` and `df2` on their (identical) index
    """
    # Ensure that indices are equal.
    dbg.dassert(
        df2.index.equals(df1.index),
        "Dataframe indices differ but are expected to be the same!",
    )
    # Ensure that there are no column duplicates within a dataframe.
    dbg.dassert_no_duplicates(df1.columns)
    dbg.dassert_no_duplicates(df2.columns)
    # Do not allow column collisions.
    dbg.dassert_not_in(
        df1.columns.to_list(),
        df2.columns.to_list(),
        "Column names overlap.",
    )
    # Ensure that column depth is equal.
    dbg.dassert_eq(
        df1.columns.nlevels,
        df2.columns.nlevels,
        msg="Column hierarchy depth must be equal.",
    )
    df = df2.merge(
        df1,
        how="outer",
        left_index=True,
        right_index=True,
    )
    return df


def validate_df_indices(df: pd.DataFrame) -> None:
    """
    Assert if `df` fails index sanity checks.
    """
    dbg.dassert_isinstance(df, pd.DataFrame)
    dbg.dassert_no_duplicates(df.columns.tolist())
    # TODO(*): assert if the datetime index has dups.


def convert_to_list(to_list: NodeColumnList) -> List[NodeColumn]:
    """
    Return a list given its input.

    - If the input is a list, the output is the same list.
    - If the input is a function that returns a list, then the output of
      the function is returned.

    How this might arise in practice:
    - A `ColumnTransformer` returns a number of x variables, with the
      number dependent upon a hyperparameter expressed in config
    - The column names of the x variables may be derived from the input
      dataframe column names, not necessarily known until graph execution
      (and not at construction)
    - The `ColumnTransformer` output columns are merged with its input
      columns (e.g., x vars and y vars are in the same dataframe)
    Post-merge, we need a way to distinguish the x vars and y vars.
    Allowing a callable here allows us to pass in the `ColumnTransformer`'s
    method `transformed_col_names()` and defer the call until graph
    execution.
    """
    if callable(to_list):
        to_list = to_list()
    if isinstance(to_list, list):
        # Check that the list is not empty and has no duplicates.
        dbg.dassert_lte(1, len(to_list))
        dbg.dassert_no_duplicates(to_list)
        return to_list
    raise TypeError("Data type=`%s`" % type(to_list))


def get_forward_cols(
    df: pd.DataFrame,
    cols: Union[List[NodeColumn], Tuple[NodeColumn]],
    steps_ahead: int,
) -> pd.DataFrame:
    """
    Obtain forward data values by shifting.

    WARNING: This function is non-causal for positive values of `steps_ahead`.
        It is intended to be used for the training stage of models that predict
        future values.

    :param df: input dataframe
    :param cols: column to generate forward values for
        - The `Tuple` type is useful for multiindexed columns
        - The `List` type should be used with single-level columns
    :param steps_ahead: number of shifts
    :return: dataframe of `steps_ahead` forward values of `df[col]`
    """
    if df.columns.nlevels == 1:
        dbg.dassert_isinstance(cols, list)
    else:
        dbg.dassert_isinstance(cols, tuple)
    # Append to the column names the number of steps ahead generated.
    mapper = lambda x: str(x) + "_%i" % steps_ahead
    forward_df = df[cols].shift(-steps_ahead).rename(columns=mapper)
    dbg.dassert_not_intersection(forward_df.columns, df.columns)
    return forward_df


def get_x_and_forward_y_fit_df(
    df: pd.DataFrame,
    x_cols: List[NodeColumn],
    y_cols: List[NodeColumn],
    steps_ahead: int,
) -> pd.DataFrame:
    """
    Return a dataframe consisting of `x_cols` and forward `y_cols`.

    This function eliminates rows that contains NaNs (either in `x_cols`
    or in the forward values of `y_cols`), which makes the resulting
    dataframe ready for use in sklearn.

    TODO(*): Consider not dropping NaNs in this function but rather
        leaving that to the caller.
    """
    validate_df_indices(df)
    # Obtain index slice for which forward targets exist.
    dbg.dassert_lt(steps_ahead, df.index.size)
    idx = df.index[:-steps_ahead]
    # Determine index where no x_vars are NaN.
    non_nan_idx_x = df.loc[idx][x_cols].dropna().index
    # Determine index where target is not NaN.
    forward_y_df = get_forward_cols(df, y_cols, steps_ahead)
    forward_y_df = forward_y_df.loc[idx].dropna()
    non_nan_idx_forward_y = forward_y_df.dropna().index
    # Intersect non-NaN indices.
    non_nan_idx = non_nan_idx_x.intersection(non_nan_idx_forward_y)
    # Ensure that the intersection is not empty.
    dbg.dassert(not non_nan_idx.empty)
    # Define the dataframes of x and forward y values.
    x_df = df.loc[non_nan_idx][x_cols]
    forward_y_df = forward_y_df.loc[non_nan_idx]
    # Merge x and forward y dataframes into one.
    df_out = merge_dataframes(x_df, forward_y_df)
    return df_out


def get_x_and_forward_y_predict_df(
    df: pd.DataFrame,
    x_cols: List[NodeColumn],
    y_cols: List[NodeColumn],
    steps_ahead: int,
) -> pd.DataFrame:
    """
    Return a dataframe consisting of `x_cols` and forward `y_cols`.

    Differs from `fit` version in that there is no requirement here that the
    forward y values be non-NaN.

    TODO(Paul): Consider combining with `get_x_and_forward_y_fit_df()` and
        parametrizing instead.
    """
    validate_df_indices(df)
    # Determine index where no x_vars are NaN.
    x_df = df[x_cols].dropna()
    non_nan_idx_x = x_df.index
    dbg.dassert(not non_nan_idx_x.empty)
    # Determine index where target is not NaN.
    forward_y_df = get_forward_cols(df, y_cols, steps_ahead)
    forward_y_df = forward_y_df.loc[non_nan_idx_x]
    # Merge x and forward y dataframes into one.
    df_out = merge_dataframes(x_df, forward_y_df)
    return df_out
