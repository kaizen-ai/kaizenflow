# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
# https://s3fs.readthedocs.io/en/latest/

# %%
import s3fs

import helpers.s3 as hs3

# %%
profile = "am"
aws_access_key_id, aws_secret_access_key, aws_region = hs3.get_aws_credentials(
    profile=profile
)

# %%
s3 = s3fs.S3FileSystem(anon=False, key=aws_access_key_id, secret=aws_secret_access_key)

# %%
bucket = hs3.get_bucket()
print("bucket=%s" % bucket)
s3.ls(bucket)

# %%
import pandas as pd

date = "2010-01-01"
start_date = pd.Timestamp(date, tz="America/New_York")
start_date.replace(hour=23, minute=30)

#pd.date_range()
