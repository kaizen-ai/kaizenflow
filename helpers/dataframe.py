"""
Import as:

import helpers.dataframe as hdf
"""

import collections
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

import helpers.dbg as dbg
import helpers.printing as prnt

_LOG = logging.getLogger(__name__)


def filter_data(
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
    # Create filter masks for each column.
    masks = []
    for col_name, vals in filters.items():
        dbg.dassert_isinstance(vals, tuple)
        mask = data[col_name].isin(vals)
        info[f"n_{col_name}"] = mask.sum()
        info[f"perc_{col_name}"] = prnt.perc(mask.sum(), data.shape[0])
        masks.append(mask)
    masks = pd.concat(masks, axis=1)
    # Combine masks.
    if mode == "and":
        combined_mask = masks.all(axis=1)
    elif mode == "or":
        combined_mask = masks.any(axis=1)
    else:
        raise ValueError("Invalid `mode`='%s'" % mode)
    if combined_mask.sum() == 0:
        _LOG.warning("No data is left after filtering.")
    filtered_data = data.loc[combined_mask].copy()
    info["nrows_left"] = filtered_data.shape[0]
    return filtered_data


def filter_data_by_comparison(
    data: pd.DataFrame,
    filters: List[Tuple[Union[int, str], str, Any]],
    mode: str,
    info: Optional[collections.OrderedDict] = None,
) -> pd.DataFrame:
    """
    Filter dataframe by comparing columns to values.

    :param data: dataframe
    :param filters: `[(col_name, comparison_method, value)]`.
        `comparison_method` is one of the ("eq", "ne", "le", "lt", "ge", "gt")
        pandas method names.
    :param mode: `and` for conjunction and `or` for disjunction of filters
    :param info: information storage
    :return: filtered dataframe
    """
    if info is None:
        info = collections.OrderedDict()
    info["nrows"] = data.shape[0]
    # Create filter masks for each column.
    masks = []
    for col_name, comparison_method, val in filters:
        dbg.dassert_in(comparison_method, ("eq", "ne", "le", "lt", "ge", "gt"))
        mask = getattr(data[col_name], comparison_method)(val)
        info[f"n_{col_name}_{comparison_method}_{val}"] = mask.sum()
        info[f"perc_{col_name}_{comparison_method}_{val}"] = prnt.perc(
            mask.sum(), data.shape[0]
        )
        masks.append(mask)
    masks = pd.concat(masks, axis=1)
    # Combine masks.
    if mode == "and":
        combined_mask = masks.all(axis=1)
    elif mode == "or":
        combined_mask = masks.any(axis=1)
    else:
        raise ValueError("Invalid `mode`='%s'" % mode)
    if combined_mask.sum() == 0:
        _LOG.warning("No data is left after filtering.")
    filtered_data = data.loc[combined_mask].copy()
    info["nrows_left"] = filtered_data.shape[0]
    return filtered_data
