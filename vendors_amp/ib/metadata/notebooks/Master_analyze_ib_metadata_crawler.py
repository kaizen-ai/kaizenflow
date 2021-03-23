# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.4.2
#   kernelspec:
#     display_name: Python [conda env:.conda-p1_develop] *
#     language: python
#     name: conda-env-.conda-p1_develop-py
# ---

# %%
import os

import pandas as pd

# %%
# !ls ../log_2021_03_11

# %%
dir_name = "../log_2021_03_11"

file_name = os.path.join(dir_name, "symbols.csv")
symbols = pd.read_csv(file_name, sep="\t")

symbols.head()

# %%
file_name = os.path.join(dir_name, "exchanges.csv")
exchanges = pd.read_csv(file_name, sep="\t")

exchanges.head()

# %%
markets = symbols["market"].unique()
print("\n".join(markets))

# %%
grouped = symbols.groupby("market")

grouped[["product"]].count()

# %%
idx = 3
print("market=", markets[idx])
mask = df["market"] == markets[idx]
symbols_tmp = symbols[mask]

grouped = symbols_tmp.groupby("product")

grouped[["product"]].count()
