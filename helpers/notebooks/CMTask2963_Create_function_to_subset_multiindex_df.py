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

# %% [markdown]
# # Create DataFrame

# %%
timestamp_index1 = [
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
df = pd.DataFrame(nums, index=timestamp_index1, columns=index)
#
df


# %% [markdown]
# # Define filtering function

# %%
def subset_multiindex_df(
    df: pd.DataFrame, timestamp=None, column_name=None, asset_id=None
) -> pd.DataFrame:
    """
    Filter DataFrame with column MultiIndex by timestamp index, and column
    levels.

    :param timestamp: either one specific date or range between two dates
    :param column_name: data-specific column name (e.g., `close`)
    :param asset_id: asset-specific column name (e.g., `binance::BTC_USDT`)
    :return: filtered DataFrame
    """
    # Filter by timestamp.
    # TODO(Max): Add an assertion that len(given timestamps)<=2.
    # TODO(Max): Add an assertion for original timestamp range vs. given one.
    if timestamp:
        if len(timestamp) == 1:
            df = df.loc[timestamp]
        else:
            df = df.loc[timestamp[0] : timestamp[1]]
    # Filter by column name.
    # TODO(Max): Add an assertion that given column names are in original columns.
    if column_name:
        df = df.swaplevel(axis=1)[column_name].swaplevel(axis=1)
    # Filter by asset_id.
    # TODO(Max): Add an assertion that given asset ids are in original columns.
    if asset_id:
        df = df[asset_id]
    return df


# %% [markdown]
# # Examples

# %%
subset_multiindex_df(
    df,
    timestamp=[
        pd.Timestamp("2022-01-01 21:01:00+00:00"),
        pd.Timestamp("2022-01-01 21:03:00+00:00"),
    ],
    column_name=["high", "low"],
    asset_id=["asset1"],
)

# %%
subset_multiindex_df(
    df,
    timestamp=[
        pd.Timestamp("2022-01-01 21:01:00+00:00"),
        pd.Timestamp("2022-01-01 21:03:00+00:00"),
    ],
    column_name=["close"],
    asset_id=["asset2"],
)

# %%
subset_multiindex_df(
    df,
    timestamp=[
        pd.Timestamp("2022-01-01 21:01:00+00:00"),
    ],
    column_name=["open", "close"],
)
