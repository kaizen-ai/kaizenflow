"""
Import as:

import core.signal_processing.fir_utils as csprfiut
"""

import logging
from typing import Any, Callable, Dict, Optional, Union

import pandas as pd

import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def extract_fir_filter_weights(
    signal: Union[pd.DataFrame, pd.Series],
    func: Callable[[pd.Series], pd.Series],
    func_kwargs: Dict[str, Any],
    warmup_length: int,
    index_location: Optional[Any] = None,
) -> pd.DataFrame:
    """
    Return weights used in a FIR filter up to `index_location`.

    This can be used in isolation to inspect filter weights, or can be used on
    data to, e.g., generate training data weights.

    :param signal: data that provides an index (for reindexing). No column
        values used.
    :param func: a function that transforms a series into a series; if not
        a FIR filter, then the "weights" of the output may need to be
        interpreted differently
    :param func_kwargs: kwargs to forward to `func`, e.g., filtering parameters
    :param warmup_length: a value that is greater than or equal to the filter
        length
    :param index_location: current and latest value to be considered operated
        upon by the filter (e.g., the last in-sample index). If `None`, then use
        the last index location of `signal`. Useful for adding weights to
        training data.
    :return: dataframe with two columns of weights:
        1. raw weights
        2. normalized weights (sum of absolute weights equal to one)
        2. relative weights (weight at `index_location` is equal to `1`, and
           prior weights are expressed relative to this value)
    """
    idx = signal.index
    hdbg.dassert_isinstance(idx, pd.Index)
    hdbg.dassert(not idx.empty, msg="`signal.index` must be nonempty.")
    index_location = index_location or idx[-1]
    if index_location > idx[-1]:
        _LOG.warning(
            "Requested `index_location` is out-of-range. "
            "Proceeding with last `signal.index` location instead."
        )
        index_location = idx[-1]
    hdbg.dassert_in(
        index_location,
        idx,
        msg="`index_location` must be a member of `signal.index`",
    )
    hdbg.dassert_lt(0, warmup_length)
    # Build an impulse series.
    # - This is a sequence of zeros with a single value equal to one.
    # - The length of the zeros is at least as long as the length of the
    #   weight series implicitly asked for by the caller. If this is less than
    #   the warm-up length, then we extend the zeros so that we can calculate
    #   reliable absolute weights
    desired_length = signal.loc[:index_location].shape[0]
    length = max(desired_length, warmup_length)
    impulse = pd.Series(0, range(0, warmup_length + length))
    impulse.iloc[warmup_length - 1] = 1
    # Apply the filter function to the step function.
    filtered_impulse = func(impulse, **func_kwargs)
    # Drop the warm-up data from the filtered series.
    filtered_impulse = filtered_impulse.iloc[warmup_length - 1 :]
    filtered_impulse.name = "weight"
    # Calculate normalized weights.
    normalized_weights = (filtered_impulse / filtered_impulse.abs().sum()).rename(
        "normalized_weight"
    )
    relative_weights = (
        filtered_impulse / abs(filtered_impulse.loc[warmup_length - 1])
    ).rename("relative_weight")
    # Build a `weights` dataframe.
    weights = pd.concat(
        [filtered_impulse, normalized_weights, relative_weights], axis=1
    ).reset_index(drop=True)
    # Truncate to `desired_length`, determined by `signal.index` and
    # `index_location`.
    weights = weights.iloc[:desired_length]
    # Reverse the series (because the weights apply to historical
    # observations).
    weights = weights.iloc[::-1].reset_index(drop=True)
    # Index and align the weights so that they terminate at `index_location`.
    weights.index = signal.loc[:index_location].index
    # Extend `weights` with NaNs if necessary.
    return weights.reindex(idx)
