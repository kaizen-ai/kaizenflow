"""
Import as:

import dataflow.model.parquet_utils as dtfmopauti
"""
from typing import Union

import numpy as np
import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas


def process_parquet_read_df(
    df: pd.DataFrame,
    asset_id_col: str,
) -> pd.DataFrame:
    """
    Post-process a multiindex dataflow result dataframe re-read from parquet.

    :param df: dataframe in "long" format
    :param asset_id_col: asset id column to pivot on
    :return: multiindexed dataframe with asset id's at the inner column level
    """
    # TODO(Paul): Maybe wrap `hparque.from_parquet()`.
    hdbg.dassert_isinstance(asset_id_col, str)
    hdbg.dassert_in(asset_id_col, df.columns)
    # Parquet uses categoricals; cast the asset ids to their native integers.
    df[asset_id_col] = df[asset_id_col].astype("int64")
    # Check that the asset id column is now an integer column.
    hpandas.dassert_series_type_is(df[asset_id_col], np.int64)
    # If a (non-asset id) column can be represented as an int, then do so.
    df = df.rename(columns=_maybe_cast_to_int)
    # Convert from long format to column-multiindexed format.
    df = df.pivot(columns=asset_id_col)
    # NOTE: the asset ids may already be sorted and so this may not be needed.
    df.sort_index(axis=1, level=-2, inplace=True)
    return df


def _maybe_cast_to_int(string: str) -> Union[str, int]:
    hdbg.dassert_isinstance(string, str)
    try:
        val = int(string)
    except ValueError:
        val = string
    return val
