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


def _ts_to_str(ts: pd.Timestamp) -> str:
    """
    Print timestamp as string only in terms of time.

    This is useful to simplify the debug output for intraday trading.
    """
    val = "'%s'" % str(ts.time())
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
    df_5mins: pd.DataFrame,
    accounting: Accounting,
    prefix: str,
) -> pd.DataFrame:
    """
    Update the df with the intermediate results stored in `accounting`.
    """
    dfs = []
    for key, value in accounting.items():
        _LOG.debug("key=%s", key)
        # Pad the data to the same length of
        num_vals = len(accounting[key])
        num_pad = df_5mins.shape[0] - num_vals
        hdbg.dassert_lte(0, num_pad)
        buffer = [np.nan] * num_pad
        # Get the target column.
        col_name = _get_col_name(key, prefix)
        # Create the column of the data frame.
        df = pd.DataFrame(
            value + buffer, index=df_5mins.index, columns=[col_name]
        )
        hdbg.dassert_eq(df.shape[0], df_5mins.shape[0])
        dfs.append(df)
    # Concat all the data together with the input.
    df_5mins = pd.concat([df_5mins] + dfs, axis=1)
    return df_5mins
