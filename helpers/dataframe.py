"""
Import as:

import helpers.dataframe as hdf
"""

import collections
import logging
from typing import Any, Dict, Optional, Tuple, Union

import numpy as np
import pandas as pd

import helpers.dbg as dbg
import helpers.printing as prnt

_LOG = logging.getLogger(__name__)


def filter_data_by_values(
    data: pd.DataFrame,
    filters: Dict[Union[int, str], Tuple[Any, ...]],
    mode: str,
    info: Optional[collections.OrderedDict] = None,
) -> pd.DataFrame:
    """
    Filter dataframe rows based on column values.

    :param data: dataframe
    :param filters: `{col_name: (possible_values)}`
    :param mode: `and` for conjunction and `or` for disjunction of filters
    :param info: information storage
    :return: filtered dataframe
    """
    if info is None:
        info = collections.OrderedDict()
    info["nrows"] = data.shape[0]
    if not filters:
        info["nrows_remaining"] = data.shape[0]
        return data.copy()
    # Create filter masks for each column.
    masks = []
    for col_name, vals in filters.items():
        dbg.dassert_isinstance(vals, tuple)
        mask = data[col_name].isin(vals)
        info[f"n_{col_name}"] = mask.sum()
        info[f"perc_{col_name}"] = prnt.perc(mask.sum(), data.shape[0])
        masks.append(mask)
    masks = pd.concat(masks, axis=1)
    combined_mask = _combine_masks(masks, mode, info)
    filtered_data = data.loc[combined_mask].copy()
    return filtered_data


def filter_data_by_comparison(
    data: pd.DataFrame,
    filters: Dict[str, Union[Tuple[str, Any], Tuple[Tuple[str, Any], ...]]],
    mode: str,
    info: Optional[collections.OrderedDict] = None,
) -> pd.DataFrame:
    """
    Filter dataframe by comparing columns to values.

    :param data: dataframe
    :param filters: `{col_name: (comparison_method, value)}` or
        `{col_name: ((comparison_method_i, value_i))}`.
        `comparison_method` is one of the ("eq", "ne", "le", "lt", "ge", "gt")
        pandas method names.
    :param mode: `and` for conjunction and `or` for disjunction of filters
    :param info: information storage
    :return: filtered dataframe
    """
    if info is None:
        info = collections.OrderedDict()
    info["nrows"] = data.shape[0]
    if not filters:
        info["nrows_remaining"] = data.shape[0]
        return data.copy()
    # Create filter masks for each column.
    masks = []
    for col_name, tuple_ in filters.items():
        if not isinstance(tuple_[0], tuple):
            tuple_ = (tuple_,)
        for comparison_method, val in tuple_:
            dbg.dassert_in(
                comparison_method, ("eq", "ne", "le", "lt", "ge", "gt")
            )
            mask = getattr(data[col_name], comparison_method)(val)
            info[f"n_{col_name}_{comparison_method}_{val}"] = mask.sum()
            info[f"perc_{col_name}_{comparison_method}_{val}"] = prnt.perc(
                mask.sum(), data.shape[0]
            )
            masks.append(mask)
    masks = pd.concat(masks, axis=1)
    combined_mask = _combine_masks(masks, mode, info)
    filtered_data = data.loc[combined_mask].copy()
    return filtered_data


def _combine_masks(
    masks: pd.DataFrame, mode: str, info: collections.OrderedDict
) -> pd.Series:
    if mode == "and":
        combined_mask = masks.all(axis=1)
    elif mode == "or":
        combined_mask = masks.any(axis=1)
    else:
        raise ValueError("Invalid `mode`='%s'" % mode)
    if combined_mask.sum() == 0:
        _LOG.warning("No data remaining after filtering.")
    info["nrows_remaining"] = combined_mask.sum()
    return combined_mask


def apply_nan_mode(
    srs: pd.Series, nan_mode: Optional[str] = None, info: Optional[dict] = None,
) -> pd.Series:
    """
    Process NaN values in a series according to the parameters.

    :param srs: pd.Series to process
    :param nan_mode: method of processing NaNs
        - None - no transformation
        - "ignore" - drop all NaNs
        - "ffill" - forward fill not leading NaNs
        - "ffill_and_drop_leading" - do ffill and drop leading NaNs
        - "fill_with_zero" - fill NaNs with 0
        - "strict" - raise ValueError that NaNs are detected
    :param info: information storage
    :return: transformed copy of input series
    """
    dbg.dassert_isinstance(srs, pd.Series)
    if srs.empty:
        _LOG.warning("Empty input series `%s`", srs.name)
    if nan_mode is None:
        res = srs.copy()
    elif nan_mode == "ignore":
        res = srs.dropna().copy()
    elif nan_mode == "ffill":
        res = srs.ffill().copy()
    elif nan_mode == "ffill_and_drop_leading":
        res = srs.ffill().dropna().copy()
    elif nan_mode == "fill_with_zero":
        res = srs.fillna(0).copy()
    elif nan_mode == "strict":
        res = srs.copy()
        if srs.isna().any():
            raise ValueError(f"NaNs detected in nan_mode `{nan_mode}`")
    else:
        raise ValueError(f"Unrecognized nan_mode `{nan_mode}`")
    #
    if info is not None:
        dbg.dassert_isinstance(info, dict)
        # Dictionary should be empty.
        dbg.dassert(not info)
        info["series_name"] = srs.name
        info["num_elems_before"] = len(srs)
        info["num_nans_before"] = np.isnan(srs).sum()
        info["num_elems_removed"] = len(srs) - len(res)
        info["num_nans_imputed"] = (
            info["num_nans_before"] - info["num_elems_removed"]
        )
        info["percentage_elems_removed"] = (
            100.0 * info["num_elems_removed"] / info["num_elems_before"]
        )
        info["percentage_elems_imputed"] = (
            100.0 * info["num_nans_imputed"] / info["num_elems_before"]
        )
    return res


def infer_sampling_points_per_year(data: Union[pd.Series, pd.DataFrame]) -> float:
    """
    Return the number of index time points per year.

    TODO(*): Consider extending to all frequencies and count points by
        explicitly building indices of the given frequency.

    :param data: series or dataframe with non-null `data.index.freq`
    :return: number of time points per year (approximate)
    """
    dbg.dassert(data.index.freq)
    freq = data.index.freq
    if freq == "D":
        points_per_year = 365
    elif freq == "B":
        points_per_year = 252
    elif freq == "W":
        points_per_year = 52
    elif freq == "M":
        points_per_year = 12
    else:
        raise ValueError(f"Unsupported freq=`{freq}`")
    return points_per_year
