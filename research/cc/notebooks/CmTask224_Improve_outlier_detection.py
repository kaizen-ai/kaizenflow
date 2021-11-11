# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Description

# %% [markdown]
# This notebook explores approaches to detect outliers in crypto data.

# %% [markdown]
# # Imports

# %%
import logging
import os

import pandas as pd

import core.config.config_ as ccocon
import helpers.dbg as hdbgremove_rolling_outliers
import helpers.env as henv
import helpers.printing as hprintin
import helpers.s3 as hs3
import im.ccxt.data.load.loader as cdlloa
import im.data.universe as imdauni
import research.cc.statistics as rccsta

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprintin.config_notebook()


# %% [markdown]
# # Functions

# %%
def remove_outlier_at_index(
    srs: pd.Series,
    z_score_boundary: int,
    z_score_window_size: int,
    index_to_check: int,
) -> pd.Series:
    """
    Check if a series value at index is an outlier and remove it if so.
    
    Index should be a row of positive integers like 0, 1, 2, etc. 
    
    Z-score window indices are adjusting with respect to its size, the size of input
    and index to check.
    
    Z-score window size is an integer number of index steps that will be included
    in Z-score computation and outlier detection.

    :param srs: input series
    :param z_score_boundary: boundary value to check for outlier's Z-score
    :param z_score_window_size: size of the window to compute Z-score for
    :param index_to_check: index of a value to check
    :return: input series with removed value at given index if it was considered an outlier
    """
    # Set window indices. 
    window_first_index = max(0, index_to_check - z_score_window_size)
    window_last_index = max(index_to_check, window_first_index + z_score_window_size)
    # Verify that distance between window indices equals Z-score window size
    # and that index to check is laying between these indices.
    hdbg.dassert_eq(z_score_window_size, window_last_index - window_first_index)
    hdbg.dassert_lte(window_first_index, index_to_check)
    hdbg.dassert_lte(index_to_check, window_last_index)
    # Get a window to compute Z-score for.
    window_srs = srs[window_first_index : window_last_index].copy()
    # Compute Z-score of a value at index.
    z_score = (srs[index_to_check] - window_srs.mean()) / window_srs.std()
    # Drop the value if its Z-score is None or laying beyond the specified boundaries.
    if not abs(z_score) <= z_score_boundary:
        srs = srs.drop([index_to_check]).copy()
    return srs


def remove_rolling_outliers(
    df: pd.DataFrame,
    col: str,
    z_score_boundary: int,
    z_score_window: int,
) -> pd.DataFrame:
    """
    Remove outliers using a rolling window.
    
    Outliers are being removed consequtively after every window check.
    
    Z-score window indices are adjusting with respect to its size, the size of input
    and index to check.
    
    Z-score window size is an integer number of index steps that will be included
    in Z-score computation and outlier detection.

    :param srs: input dataframe
    :param col: column to check for outliers
    :param z_score_boundary: Z-score boundary to check the value
    :param z_score_window: size of the window to compute Z-score for
    :return: dataframe with removed outliers
    """
    # If index is a timestamp, reset it and name accordingly.
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index().rename(columns={"index":"timestamp"})
    hdbg.dassert_in("timestamp", df.columns)
    # Get a series to detect outliers in.
    price_srs = df[col].copy()
    # Iterate over series indices.
    for index_ in price_srs.index:
        # For every index check if its value is an outlier and
        # remove it from the series if so.
        price_srs = remove_outlier_at_index(
            price_srs, z_score_boundary, z_score_window, index_
        )
    # Get dataframe rows that correspond to the non-outliers indices.
    clean_df = df[df.index.isin(price_srs.index)].copy()
    # Set timestamp as index.
    clean_df = clean_df.set_index("timestamp")
    return clean_df


# %%
root_dir = "s3://alphamatic-data/data"
сcxt_loader = cdlloa.CcxtLoader(root_dir=root_dir, aws_profile="am")
data = сcxt_loader.read_data_from_filesystem("kucoin", "ETH/USDT", "ohlcv")
data.head()

# %% [markdown]
# The problem with the proposed approach is that it is very slow - it takes ~5.7 second to process a chunk with 20.000 rows while the original data size is 75 times higher (1.500.000 / 20.000) which results to ~7 min per exchange-currency data.

# %%
data_chunk = data.tail(20000).copy()

# %%
# %%time
clean_data_chunk = remove_rolling_outliers(data_chunk, "close", 3, 60)

# %%
print(len(clean_data_chunk))
clean_data_chunk.head()

# %%
outliers = data_chunk[~data_chunk.index.isin(clean_data_chunk.index)].copy()
#
print(len(outliers))
outliers

# %%
