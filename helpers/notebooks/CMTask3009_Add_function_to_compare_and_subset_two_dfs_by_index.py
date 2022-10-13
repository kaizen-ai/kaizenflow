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

import helpers.hpandas as hpandas

# %%
timestamp_index1 = [
    pd.Timestamp("2022-01-01 21:00:00+00:00"),
    pd.Timestamp("2022-01-01 21:01:00+00:00"),
    pd.Timestamp("2022-01-01 21:02:00+00:00"),
    pd.Timestamp("2022-01-01 21:03:00+00:00"),
    pd.Timestamp("2022-01-01 21:04:00+00:00"),
    pd.Timestamp("2022-01-01 21:05:00+00:00"),
    pd.Timestamp("2022-01-01 21:06:00+00:00"),
    pd.Timestamp("2022-01-01 21:06:00+00:00"),
]
timestamp_index2 = [
    pd.Timestamp("2022-01-01 21:02:00+00:00"),
    pd.Timestamp("2022-01-01 21:03:00+00:00"),
    pd.Timestamp("2022-01-01 21:04:00+00:00"),
    pd.Timestamp("2022-01-01 21:05:00+00:00"),
]
timestamp_index3 = [
    pd.Timestamp("2022-01-01 21:01:00+00:00"),
    pd.Timestamp("2022-01-01 21:02:00+00:00"),
    pd.Timestamp("2022-01-01 21:03:00+00:00"),
    pd.Timestamp("2022-01-01 21:04:00+00:00"),
]
#
value1 = {"value1": [None, None, 1, 2, 3, 4, 5, None]}
value2 = {"value2": [1, 2, 3, None]}
value3 = {"value3": [None, None, 1, 2]}
#
df1 = pd.DataFrame(value1, index=timestamp_index1)
df2 = pd.DataFrame(value2, index=timestamp_index2)
df3 = pd.DataFrame(value3, index=timestamp_index3)

# %%
tag_to_df = {
    "tag1": df1,
    "tag2": df2,
    "tag3": df3,
}
# 
tag_to_df

# %%
hpandas.compute_duration_df(tag_to_df, valid_intersect=True, intersect_dfs=True)

# %%
tag_to_df
