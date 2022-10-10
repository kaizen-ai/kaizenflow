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

# %% [markdown]
# # Imports

# %%
import pandas as pd
import numpy as np

import helpers.hpandas as hpandas


# %% [markdown]
# # Create Multiindex DataFrames

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
df1 = pd.DataFrame(nums, index=timestamp_index1, columns=index)
#
display(df1)

# %%
timestamp_index2 = [
    pd.Timestamp("2022-01-01 21:00:00+00:00"),
    pd.Timestamp("2022-01-01 21:01:00+00:00"),
    pd.Timestamp("2022-01-01 21:02:00+00:00"),
    pd.Timestamp("2022-01-01 21:03:00+00:00"),
    pd.Timestamp("2022-01-01 21:04:00+00:00"),
    pd.Timestamp("2022-01-01 21:05:00+00:00"),
    pd.Timestamp("2022-01-01 21:06:00+00:00"),
    pd.Timestamp("2022-01-01 21:06:00+00:00"),
]
#
iterables = [["asset1", "asset2", "asset3"], ["open", "high", "low", "close", "volume"]]
index = pd.MultiIndex.from_product(iterables, names=[None, "timestamp"])
#
nums = [[ 0.79095104, -0.10304008, -0.69848962,  0.50078409,  0.41756371,
        -1.33487885,  1.04546138,  0.191062  ,  0.08841533,  0.61717725,
        -2.15558483,  1.21036169,  2.60355386,  0.07508052,  1.00702849],
       [ 0.56223723,  0.97433151, -1.40471182,  0.53292355,  0.24381913,
         0.64343069, -0.46733655, -1.20471491, -0.08347491,  0.33365524,
         0.04370572, -0.53547653, -1.07622168,  0.7318155 , -0.47146482],
       [-0.48272741,  1.17859032, -0.40816664,  0.46684297,  0.42518077,
        -1.52913855,  1.09925095,  0.48817537,  1.2662552 , -0.59757824,
         0.23724902, -0.00660826,  0.09780482, -0.17166633, -0.54515917],
       [-0.37618442, -0.3086281 ,  1.09168123, -1.1751162 ,  0.38291194,
         1.80830268,  1.28318855,  0.75696503, -1.04042572,  0.06493231,
        -0.10392893,  1.89053412, -0.21200498,  1.61212857, -2.00765278],
       [-0.19674075, -1.02532132, -0.22486018,  0.37664998,  0.35619408,
        -0.77304675,  0.59053699, -1.53249898,  0.57548424, -0.32093537,
        -0.52109972,  1.70938034, -0.55419632,  0.45531674,  0.66878119],
       [ 0.05903553,  1.2040308 ,  0.62323671, -0.23639535,  0.87270792,
         2.60253287, -0.77788842,  0.80645833,  1.85438743, -1.77561587,
         0.41469478, -0.29791883,  0.75140743,  0.50389702,  0.55311024],
       [-0.97820763, -1.32155197, -0.6143911 ,  0.01473404,  0.87798665,
         0.1701048 , -0.75376376,  0.72503616,  0.5791076 ,  0.43942739,
         0.62505817,  0.44998739,  0.37350664, -0.73485633, -0.70406184],
       [-1.35719477, -1.82401288,  0.77263763,  2.36399552, -0.45353019,
         0.33983713, -0.62895329,  1.34256611,  0.2207564 ,  0.24146184,
         0.90769186,  0.57426869, -0.04587782, -1.6319128 ,  0.38094798]]
df2 = pd.DataFrame(nums, index=timestamp_index2, columns=index)
display(df2)


# %% [markdown]
# # Functions

# %%
def compare_multiindex_dfs(
    df1: pd.DataFrame, 
    df2: pd.DataFrame, 
    intersection_columns: bool = False,
    filter_columns: bool = False,
    custom_columns_level0: list = None,
    custom_columns_level1: list = None,
    sort_columns: bool = False,
    intersection_index: bool = False,
    filter_index: bool = False,
    custom_start_timestamp: list = None,
    custom_end_timestamp: list = None,
):
    """
    :param intersection_columns: filter by the columns that are equal to both DataFrames
    :param filter_columns: filter by the custom set of columns
    :param custom_columns_level0: if "filter_columns" is True -> filter by given columns
    :param custom_columns_level1: same as `custom_columns_level0` but for level 1
    :param sort_columns: sort columns by alphabetical order
    :param intersection_index: same as `intersection_columns` but for index
    :param filter_index: same as `filter_columns` but for index
    :param custom_start_timestamp: if "filter_index" is True -> filter by given range 
    :param custom_end_timestamp: same as `custom_start_timestamp` but for end timestamp
    """
    # Check columns and filter by request.
    columns_equality = np.array_equal(np.array(df1.columns), np.array(df2.columns))
    if not columns_equality:
        if intersection_columns:
            inters_columns = df1.columns.intersection(df2.columns)
            df1 = df1[inters_columns]
            df2 = df2[inters_columns]
        if filter_columns:
            df1 = hpandas.subset_multiindex_df(
                df1, 
                columns_level0=custom_columns_level0,
                columns_level1=custom_columns_level1,
            )
            df2 = hpandas.subset_multiindex_df(
                df2, 
                columns_level0=custom_columns_level0,
                columns_level1=custom_columns_level1,
            )
        if sort_columns:
            df1 = df1.reindex(sorted(df1.columns), axis=1)
            df2 = df2.reindex(sorted(df2.columns), axis=1)
    # Check index and filter by request.
    index_equality = np.array_equal(np.array(df1.index), np.array(df2.index))
    if not index_equality:
        if intersection_index:
            inters_index = df1.index.intersection(df2.index)
            df1 = df1.loc[inters_index]
            df2 = df2.loc[inters_index]
        if filter_index:
            df1 = hpandas.subset_multiindex_df(
                df1, 
                start_timestamp=custom_start_timestamp,
                end_timestamp=custom_end_timestamp,
            )
            df2 = hpandas.subset_multiindex_df(
                df2, 
                start_timestamp=custom_start_timestamp,
                end_timestamp=custom_end_timestamp,
            )
    return df1, df2


def check_diff_multiindex_dfs(
    df1,
    df2,
    columns_level0 = None,
    columns_level1 = None,
    column_mode: str ="equal",
    row_mode: str="equal",
    diff_mode: str="diff",
    background_gradient: bool = True,
) -> pd.DataFrame:
    # Subset DataFrames.
    df1 = hpandas.subset_multiindex_df(
                df1, 
                columns_level0=columns_level0,
                columns_level1=columns_level1,
    )
    df2 = hpandas.subset_multiindex_df(
                df2, 
                columns_level0=columns_level0,
                columns_level1=columns_level1,
    )        
    # Show the difference.
    df_diff = hpandas.compare_visually_dataframes(df1, df2, column_mode, row_mode, diff_mode)
    return df_diff


# %% [markdown]
# # Examples

# %%
new_df1, new_df2 = compare_multiindex_dfs(
    df1, 
    df2,
    filter_columns=True,
    custom_columns_level0 = ["asset1","asset2"],
    custom_columns_level1 = ["low", "high"],
    sort_columns = True,
    intersection_columns= True,
    filter_index = True,
    custom_start_timestamp = pd.Timestamp("2022-01-01 21:02:00+00:00"),
    custom_end_timestamp = pd.Timestamp("2022-01-01 21:04:00+00:00")
)
display(new_df1, new_df2)

# %%
new_df_diff = check_diff_multiindex_dfs(new_df1,new_df2, columns_level0=["asset1"], columns_level1=["high"])
new_df_diff
