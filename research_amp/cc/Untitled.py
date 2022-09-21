# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.8
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
import pandas as pd

import helpers.hdbg as hdbg

# %%
timestamp_index = [
    pd.Timestamp("2022-09-08 21:01:00+00:00"),
    pd.Timestamp("2022-09-08 21:02:00+00:00"),
    pd.Timestamp("2022-09-08 21:03:00+00:00"),
]
values = {
    "bid_price": pd.Series([2.31, 3.22, 2.33]),
    "bid_size": pd.Series([1.1, 2.2, 3.3]),
    "ask_price": pd.Series([2.34, 3.24, 2.35]),
    "ask_size": pd.Series([4.4, 5.5, 6.6]),
    "timestamp": timestamp_index,
}
df1 = pd.DataFrame(data=values)
df1 = df1.set_index("timestamp")

timestamp_index1 = [
    pd.Timestamp("2022-09-08 21:01:00+00:00"),
    pd.Timestamp("2022-09-08 21:02:00+00:00"),
    "Extra_row",
]
values1 = {
    "bid_price": pd.Series([2.32, 3.21, 2.23]),
    "bid_size": pd.Series([1.04, 2.76, 3.25]),
    "ask_price": pd.Series([2.32, 3.25, 2.34]),
    "ask_size": pd.Series([4.35, 5.56, 6.54]),
    "timestamp": timestamp_index1,
    "Extra_col": pd.Series(["rr", 4, 5]),
}
df2 = pd.DataFrame(data=values1)
df2 = df2.set_index("timestamp")
#
display(df1)
display(df2)


# %%
def merge_and_calculate_difference(df1, df2, diff_mode):
    suffixes = ("_1", "_2")
    merged_df = df1.merge(
        df2,
        how="outer",
        left_index=True,
        right_index=True,
        suffixes=suffixes,
    )
    merged_df = merged_df.reindex(sorted(merged_df.columns), axis=1)
    #
    i = 0
    num_initial_cols = len(merged_df.columns)
    while i != num_initial_cols:
        col_diff_name = f"{merged_df.columns[i][:-len(suffixes[0])]}_{diff_mode}"
        if diff_mode == "diff":
            merged_df[f"{col_diff_name}"] = (
                merged_df.iloc[:, i] - merged_df.iloc[:, i + 1]
            )
        elif diff_mode == "pct_change":
            merged_df[f"{col_diff_name}"] = (
                100
                * (merged_df.iloc[:, i] - merged_df.iloc[:, i + 1])
                / merged_df.iloc[:, i + 1]
            )
        i = i + 2
    return merged_df


def compare_visually_dataframes(
    df1, df2, row_mode="equal", column_mode="equal", diff_mode="diff"
):
    """
    :param row_mode: controls how the rows are handled
     - "equal": rows need to be the same
     - "inner": compute the intersection
    :param column_mode: same as row_mode
    :param diff_mode: control how the dataframes are computed
     - "diff": compute the difference between dataframes
     - "pct_change": compute the percentage change between dataframes
    """
    final_df = None
    if column_mode == "equal":
        msg = "Columns of two DataFrames are not the same!"
        hdbg.dassert_eq(list(df1.columns), list(df2.columns), msg)
    elif column_mode == "inner":
        same_cols = list(set(list(df1.columns)) & set(list(df2.columns)))
        df1 = df1[same_cols]
        df2 = df2[same_cols]
    else:
        raise ValueError("Invalid column_mode='%s'" % column_mode)

    if row_mode == "equal":
        msg = "Rows of two DataFrames are not the same!"
        hdbg.dassert_eq(list(df1.index), list(df2.index), msg)
    elif row_mode == "inner":
        same_rows = list(set(list(df1.index)) & set(list(df2.index)))
        df1 = df1[df1.index.isin(same_rows)]
        df2 = df2[df2.index.isin(same_rows)]
    else:
        raise ValueError("Invalid row_mode='%s'" % column_mode)

    final_df = merge_and_calculate_difference(df1, df2, diff_mode)
    return final_df


# %%
compare_visually_dataframes(
    df1, df2, row_mode="inner", column_mode="inner", diff_mode="diff"
)

# %%
