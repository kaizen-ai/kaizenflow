# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.5.1
#   kernelspec:
#     display_name: Python [conda env:.conda-p1_develop] *
#     language: python
#     name: conda-env-.conda-p1_develop-py
# ---

# %%
# %load_ext autoreload
# %autoreload 2

import datetime as dt
import os
from typing import Dict

import pandas as pd

import helpers.s3 as hs3
import vendors2.kibot.data.load as kdl
import vendors2.kibot.data.load.file_path_generator as fpgen
import vendors2.kibot.data.types as types

# %%
file = "s3://external-p1/kibot/sp_500_1min/AAPL.csv.gz"
#file = "s3://external-p1/kibot/sp_500_1min/A.csv.gz"

df = pd.read_csv(file)
df.head(5)

# %%
#file = "s3://external-p1/kibot/pq/all_stocks_1min/AAPL.pq"
file = "s3://external-p1/kibot/pq/sp_500_1min/AAPL.pq"

pd.read_parquet(file)
