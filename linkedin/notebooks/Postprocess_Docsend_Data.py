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
#
# This notebook is used for prototyping ways to post-process `docsend` data that has been exported to a Google Sheet in GDrive.
#
# - Getting the `docsend` data using this Zapier connection might be good if used properly in conjunction with the GSheets API: https://zapier.com/apps/docsend/integrations/google-sheets/175058/create-google-sheet-rows-for-new-visits-in-docsend

# %% [markdown]
# ## Instructions
#
# - Follow the directions here for *Client Credentials* and follow the *Service Account Route* : https://gspread-pandas.readthedocs.io/en/latest/getting_started.html
#     - Remember to save your credentials to the gspread_pandas folder
# - Get the email of your service account
#     - This can be found under Service Accounts header in the Credentials Tab of your project
# - Share your Google Sheets file with the service account email(press Share top right)
# - Rename the `gsheets_name` variable to the name of the file

# %%
# Install gspread-pandas
# !sudo /bin/bash -c "(source /venv/bin/activate; pip install gspread-pandas)"

# %%
# Imports
import numpy as np
import pandas as pd
from gspread_pandas import Spread

# %%
## Google Sheet name
gsheets_name = "Kaizen - VC presentation - v1.5-export"

# %%
## Load data
spread = Spread(gsheets_name)
data = spread.sheets[0].get_values()
headers = data.pop(0)
df = pd.DataFrame(data, columns=headers)

# %%
## Clean data
df["Duration"] = pd.to_timedelta(df["Duration"])
df = df.replace(r"^\s*$", np.nan, regex=True)
df

# %%
## Compute data
sorted_by_duration_df = df.sort_values(by="Duration", ascending=False)
sorted_by_duration_df = sorted_by_duration_df.dropna(subset=["Email"])
sorted_emails = sorted_by_duration_df["Email"].unique()

# %%
## Show data
print(sorted_emails)
