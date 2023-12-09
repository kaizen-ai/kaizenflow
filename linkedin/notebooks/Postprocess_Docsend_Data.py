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
# This notebook was used for prototyping ways to postprocess docsend data that has been exported to a Google Sheet in GDrive.
#
# - getting the docsend data using this zapier connection might be good if used properly in conjunction with the GSheets API: https://zapier.com/apps/docsend/integrations/google-sheets/175058/create-google-sheet-rows-for-new-visits-in-docsend

# %% [markdown]
# ## Instructions
#
# - Run pip install gspread-pandas in terminal
# - Choose between the GSheets and CSV methods
#
#
# ### Method 1 - GSheets
# - Follow the directions here for *Client Credentials* and follow the *Service Account Route* : https://gspread-pandas.readthedocs.io/en/latest/getting_started.html
#     - Remember to save your credentials to the gspread_pandas folder
# - Get the email of your service account
#     - This can be found under Service Accounts header in the Credentials Tab of your project
# - Share your Google Sheets file with the service account email
# - Rename the gsheets_name variable to the name of the file
# - Run notebook with Google Sheets mode
#
# ### Method 2 - CSV Download
# - Download the CSV to the current directory(the place where this notebook exists)
# - Rename the csv_file_name variable to the file name

# %%
#imports 

import sys
import pandas as pd
import numpy as np
from gspread_pandas import Spread, Client
import gspread

# %%
## Names and external imports
gsheets_name = "Kaizen - VC presentation - v1.5-export"
csv_file_name = "docsend_data.csv"

# %%
## load data
if (gsheets_name != ""):
    spread = Spread(gsheets_name)    
    data = spread.sheets[0].get_values()
    headers = data.pop(0)
    df#clean data#clean data = pd.DataFrame(data, columns=headers)
else:
    df = pd.read_csv(csv_file_name)

# %%
## clean data
df['Duration'] = pd.to_timedelta(df['Duration'])
df = df.replace(r'^\s*$', np.nan, regex=True)
df

# %%
## compute data
sorted_by_duration_df = df.sort_values(by='Duration', ascending=False)
sorted_by_duration_df = sorted_by_duration_df.dropna(subset=['Email'])
top_ten_emails = sorted_by_duration_df['Email'].unique()[:10]

# %%
## show data
print(top_ten_emails)
