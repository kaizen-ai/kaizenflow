"""
Import as:

import oms.oms_utils as oomuti
"""
import collections
import logging
from typing import Dict, List

import numpy as np
import pandas as pd

import helpers.dbg as hdbg

_LOG = logging.getLogger(__name__)


def _timestamp_to_str(timestamp: pd.Timestamp) -> str:
    """
    Print timestamp as string only in terms of time.

    This is useful to simplify the debug output for intraday trading.
    """
    val = "'%s'" % str(timestamp.time())
    return val


def _get_col_name(col_name: str, prefix: str) -> str:
    if prefix != "":
        col_name = prefix + "." + col_name
    return col_name


# #############################################################################
# Accounting functions.
# #############################################################################

# Represent a set of DataFrame columns that is built incrementally.
Accounting = Dict[str, List[float]]


def _create_accounting_stats(columns: List[str]) -> Accounting:
    """
    Create incrementally built dataframe with the given columns.
    """
    accounting = collections.OrderedDict()
    for column in columns:
        accounting[column] = []
    return accounting


def _append_accounting_df(
    df: pd.DataFrame,
    accounting: Accounting,
    prefix: str,
) -> pd.DataFrame:
    """
    Update `df` with the intermediate results stored in `accounting`.
    """
    dfs = []
    for key, value in accounting.items():
        _LOG.debug("key=%s", key)
        # Pad the data so that it has the same length as `df`.
        num_vals = len(accounting[key])
        num_pad = df.shape[0] - num_vals
        hdbg.dassert_lte(0, num_pad)
        buffer = [np.nan] * num_pad
        # Get the target column name.
        col_name = _get_col_name(key, prefix)
        # Create the column of the data frame.
        df_out = pd.DataFrame(value + buffer, index=df.index, columns=[col_name])
        hdbg.dassert_eq(df_out.shape[0], df.shape[0])
        dfs.append(df_out)
    # Concat all the data together with the input.
    df_out = pd.concat([df] + dfs, axis=1)
    return df_out
