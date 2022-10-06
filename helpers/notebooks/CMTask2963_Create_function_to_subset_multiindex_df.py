# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
import pandas as pd
import numpy as np
import helpers.hpandas as hpandas


# %% [markdown]
# # Create DataFrame

# %%
timestamp_index = [
    pd.Timestamp("2022-01-01 21:01:00+00:00"),
    pd.Timestamp("2022-01-01 21:02:00+00:00"),
    pd.Timestamp("2022-01-01 21:03:00+00:00"),
    pd.Timestamp("2022-01-01 21:04:00+00:00"),
    pd.Timestamp("2022-01-01 21:05:00+00:00"),
]
#
iterables = [["asset1", "asset2"], ["open", "high", "low", "close"]]
#
index = pd.MultiIndex.from_product(iterables, names=[None, "timestamp"])
#
nums = np.array(
    [
        [
            0.77650806,
            0.12492164,
            -0.35929232,
            1.04137784,
            0.20099949,
            1.4078602,
            -0.1317103,
            0.10023361,
        ],
        [
            -0.56299812,
            0.79105046,
            0.76612895,
            -1.49935339,
            -1.05923797,
            0.06039862,
            -0.77652117,
            2.04578691,
        ],
        [
            0.77348467,
            0.45237724,
            1.61051308,
            0.41800008,
            0.20838053,
            -0.48289112,
            1.03015762,
            0.17123323,
        ],
        [
            0.40486053,
            0.88037142,
            -1.94567068,
            -1.51714645,
            -0.52759748,
            -0.31592803,
            1.50826723,
            -0.50215196,
        ],
        [
            0.17409714,
            -2.13997243,
            -0.18530403,
            -0.48807381,
            0.5621593,
            0.25899393,
            1.14069646,
            2.07721856,
        ],
    ]
)
#
df = pd.DataFrame(nums, index=timestamp_index, columns=index)
#
df


# %% [markdown]
# # Define filtering function

# %%
def subset_multiindex_df(
    df: pd.DataFrame, start_timestamp=None, end_timestamp=None, columns_level0=None, columns_level1=None
) -> pd.DataFrame:
    """
    Filter DataFrame with column MultiIndex by timestamp index, and column
    levels.

    :param timestamp: either one specific date or range between two dates
    :param columns_level0: asset-specific column name (e.g., `binance::BTC_USDT`)
    :param columns_level1: data-specific column name (e.g., `close`)
    :return: filtered DataFrame
    """
    # Filter by timestamp.
    # TODO(Max): Add an assertion for original timestamp range vs. given one.
    df = hpandas.trim_df(
        df,
        ts_col_name = None,
        start_ts = start_timestamp,
        end_ts = end_timestamp,
        left_close = True,
        right_close= True,
    )
    # Filter by asset_id (level 0).
    # TODO(Max): Add an assertion that given asset ids are in original columns.
    if columns_level0:
        df = df[columns_level0]
    # Filter by column name (level 1).
    # TODO(Max): Add an assertion that given column names are in original columns.
    if columns_level1:
        df = df.swaplevel(axis=1)[columns_level1].swaplevel(axis=1)
    return df


# %% [markdown]
# # Examples

# %%
subset_multiindex_df(
    df,
    start_timestamp=pd.Timestamp("2022-01-01 21:01:00+00:00"),
    end_timestamp=pd.Timestamp("2022-01-01 21:03:00+00:00"),
    columns_level0=["asset1"],
    columns_level1=["high", "low"],
)

# %%
subset_multiindex_df(
    df,
    start_timestamp=pd.Timestamp("2022-01-01 21:01:00+00:00"),
    end_timestamp=pd.Timestamp("2022-01-01 21:03:00+00:00"),
    columns_level0=["asset2"],
    columns_level1=["close"],
)

# %%
subset_multiindex_df(
    df,
    start_timestamp=pd.Timestamp("2022-01-01 21:01:00+00:00"),
    end_timestamp=pd.Timestamp("2022-01-01 21:01:00+00:00"),
    columns_level1=["open", "close"],
)
