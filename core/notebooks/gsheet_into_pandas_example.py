# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.3.2
#   kernelspec:
#     display_name: Python [conda env:.conda-p1_develop] *
#     language: python
#     name: conda-env-.conda-p1_develop-py
# ---

# %% [markdown]
# # Description

# %% [markdown]
# This notebook provides an example of using the `gspread_pandas` library.
#
# The library accesses google sheets and loads them into pandas dataframes.

# %% [markdown]
# # Imports

# %%
import gspread_pandas

# %% [markdown]
# # Testing the library

# %%
spread = gspread_pandas.Spread("Task 961 - RP: Ideas for signals to test")

# %%
spread

# %%
spread.sheets

# %%
df = spread.sheet_to_df(index=None)
print(df.shape)
print(type(df))
df.head()

# %%
assets_sample = df[df["GROUP"] == "assets"]
print(assets_sample.shape)
assets_sample.head()
