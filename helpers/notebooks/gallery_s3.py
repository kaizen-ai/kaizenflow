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
# %load_ext autoreload
# %autoreload 3

# %%

import helpers.s3 as hs3

# %%
profile = "am"

# %%
# s3 = s3fs.S3FileSystem(anon=False, key=aws_access_key_id, secret=aws_secret_access_key)

s3 = hs3.get_s3fs(profile)

# %%
bucket = hs3.get_bucket()
print("bucket=%s" % bucket)
s3.ls(bucket)

# %%
s3.ls(bucket)

# %%
import os

print(os.environ["AWS_DEFAULT_REGION"])
