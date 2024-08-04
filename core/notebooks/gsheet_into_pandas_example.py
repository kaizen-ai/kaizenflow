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
# This notebook provides an example of using the `gspread_pandas` library.
#
# The library accesses google sheets and loads them into pandas dataframes.

# %% [markdown]
# # Imports

# %%
import gspread
print(gspread.__version__)

import gspread_pandas
print(gspread_pandas.__version__)

# %% [markdown]
# # Test gspread

# %%
# This is a "Test" spreadsheet.
# It needs to be shared with the email in the JSON file
# "client_email": "gp-gspread@gspread-gp.iam.gserviceaccount.com",
gsheet_url = "https://docs.google.com/spreadsheets/d/1w9qvrZF5nuhLwphMHzz8hBI59Y6rRHkh2lFa2FeUSa8/edit#gid=0"
gsheet_name = "Test"

# %%
import gspread

gc = gspread.service_account(filename="/home/.config/gspread_pandas/google_secret.json")

# %%
#sh = gc.open(gsheet_url)
sh = gc.open(gsheet_name)

print(sh.sheet1.get('A1'))

# %% [markdown]
# ## Test gspread-pandas

# %%
import gspread_pandas

# %%
#gspread_pandas.conf.get_config()
gspread_pandas.conf.get_config()["project_id"]

# %%
spread = gspread_pandas.Spread(gsheet_url)

# %%
spread

# %%
spread.sheets

# %%
df = spread.sheet_to_df(index=None)
print(df.shape)
print(type(df))
df.head()
