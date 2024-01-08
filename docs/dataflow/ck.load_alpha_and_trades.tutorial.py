# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Description

# %% [markdown]
# The notebooks loads target holdings, prices and pnl data. In addition, it shows how to compute PnL using target holdings in shares and price data.

# %% [markdown]
# # Imports

# %%
import os

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# %%
# Set default plot figsize.
plt.rcParams["figure.figsize"] = (20, 5)
# Set aspects of the visual theme for all plots.
sns.set()


# %% [markdown]
# # Functions

# %%
def load_file(dst_dir: str, file_name: str) -> pd.DataFrame:
    """
    Load file and reshape the data.

    :param dst_dir: destination dir to load a file from, e.g., /some_dir/some_subdir
    :param file_name: name of a file to load, e.g., `pnl.csv.gz`
    :return: a table containg reshaped data
    """
    # Get the file path.
    file_path = os.path.join(dst_dir, file_name)
    # Read a CSV file.
    df = pd.read_csv(file_path)
    # Convert dates to the date type.
    df["end_ts"] = pd.to_datetime(df["end_ts"])
    # Pivot the data so that timestamp becomes an index.
    df = df.pivot(index="end_ts", columns=["full_symbol"])
    return df


# %% [markdown]
# # Load data

# %%
# Replace with the destination dir to load files from.
dst_dir = "/shared_data/marketing/cmtask6334"
print(dst_dir)

# %% [markdown]
# ## Load target positions

# %%
target_holdings_file_name = "target_holdings_shares_and_prices.csv.gz"
target_holdings_df = load_file(dst_dir, target_holdings_file_name)
print(target_holdings_df.shape)
target_holdings_df.tail(3)

# %% [markdown]
# ## Load bar-by-bar PnL

# %% run_control={"marked": true}
pnl_file_name = "pnl.bar_by_bar.csv.gz"
pnl_df = load_file(dst_dir, pnl_file_name)
print(pnl_df.shape)
pnl_df.tail(3)

# %% [markdown]
# ## Load daily PnL

# %%
pnl_resampled_file_name = "pnl.daily.csv.gz"
pnl_resampled_df = load_file(dst_dir, pnl_resampled_file_name)
print(pnl_resampled_df.shape)
pnl_resampled_df.tail(3)

# %%
# Sum daily PnL across all assets and plot cumulative sum.
pnl_resampled_df.sum(axis=1, min_count=1).cumsum().plot(
    title="Cumulative PnL", ylabel="dollars"
)
plt.show()

# %% [markdown]
# # Compare research PnL with the PnL reconstructed from target holdings and prices

# %%
# Reconstruct the PnL from target holdings in shares and prices.
reconstructed_pnl = (
    target_holdings_df["target_holdings_shares"]
    .shift(2)
    .multiply(target_holdings_df["price"].diff())
)
reconstructed_pnl.tail(3)

# %%
# Ensure that both versions of PnL match. Since the target positions are computed in the bar-by-bar
# fashion, we compare them againts PnL of the same granularity.
pnl_df["pnl"].corrwith(reconstructed_pnl)
