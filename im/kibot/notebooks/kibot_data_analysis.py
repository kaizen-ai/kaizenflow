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


import pandas as pd

import helpers.pd_helpers as pdhelp
import helpers.s3 as hs3

# %%
S3_BUCKET = hs3.get_bucket()
file_name = f"s3://{S3_BUCKET}/data/kibot/sp_500_1min/AAPL.csv.gz"

df = pdhelp.read_csv(file_name, aws_profile="am")
df.head(5)

# %%
file_name = f"s3://{S3_BUCKET}/data/kibot/pq/sp_500_1min/AAPL.pq"
# TODO(gp): Create a `pdhelp.read_parquet()`.
pd.read_parquet(file_name)
