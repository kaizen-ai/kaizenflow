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
def compare_visually_dataframes(
    df1, df2, column_mode="equal", row_mode="equal", diff_mode="diff"
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
    if row_mode == "equal":
        hdbg.dassert_eq(list(df1.index), list(df2.index))
    elif row_mode == "inner":
        same_rows = list((set(df1.index)).intersection(set(df2.index)))
        df1 = df1[df1.index.isin(same_rows)]
        df2 = df2[df2.index.isin(same_rows)]
    else:
        raise ValueError("Invalid row_mode='%s'" % row_mode)
    #
    if column_mode == "equal":
        hdbg.dassert_eq(sorted(df1.columns), sorted(df2.columns))
        col_names = df1.columns
    elif column_mode == "inner":
        col_names = list(set(df1.columns).intersection(set(df2.columns)))
    else:
        raise ValueError("Invalid column_mode='%s'" % column_mode)
    #
    if diff_mode == "diff":
        df_diff = df1[col_names] - df2[col_names]
    elif diff_mode == "pct_change":
        df_diff = 100 * (df1[col_names] - df2[col_names]) / df2[col_names]
    df_diff = df_diff.add_suffix(f"_{diff_mode}")
    #
    return df_diff


# %%
compare_visually_dataframes(
    df1, df2, row_mode="inner", column_mode="inner", diff_mode="diff"
)

# %%
