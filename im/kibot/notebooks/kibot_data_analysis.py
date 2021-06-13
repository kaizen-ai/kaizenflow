# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.5.1
#   kernelspec:
#     display_name: Python [conda env:.conda-develop] *
#     language: python
#     name: conda-env-.conda-develop-py
# ---

# %%
# %load_ext autoreload
# %autoreload 2


import os
import pandas as pd

import helpers.s3 as hs3

# %%
S3_BUCKET = hs3.get_bucket()
file = f"s3://{S3_BUCKET}/data/kibot/sp_500_1min/AAPL.csv.gz"

df = pd.read_csv(file)
df.head(5)

# %%
file = f"s3://{S3_BUCKET}/data/kibot/pq/sp_500_1min/AAPL.pq"

pd.read_parquet(file)
