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
# This notebook is used to get email from DropContact API using first name,
# last name and company name. The input data is from a Google Sheet.

# %%
# !sudo /bin/bash -c "(source /venv/bin/activate; pip install --upgrade google-api-python-client)"

# %% [markdown]
# # Import

# %%
import os

import gspread_pandas
import pandas as pd

import helpers.hgoogle_file_api as hgofiapi
import marketing.dropcontact as mrkdrop

# %% [markdown]
# # Get data from Google Sheet

# %%
# Set up the Google sheet name.
gsheet_name = "Search4.FinTech_VC_in_US.SalesNavigator"
#
creds = hgofiapi.get_credentials()
spread = gspread_pandas.Spread(gsheet_name, creds=creds)
df = spread.sheet_to_df(index=None, sheet="Missing emails")[:10]
print(df.shape)
df.head()

# %% [markdown]
# # Set up

# %% run_control={"marked": true}
# Batch size is how many data we send to the API per request.
# Batch endpoint can process up to 250 contacts with a single request.
# Default batch size is set to 50, for an 1 minute processing time.
# One contact data must be less than 10 kB.
#
# The API will cost 1 credit per data length.
batch_size = 50
# The column titles for first name, last name and company name in Given GSheet.
first_name_col = "firstName"
last_name_col = "lastName"
company_col = "company"
# API key of DropContact.
api_key = os.environ["API_KEY"]

# %% [markdown]
# # Get emails from DropContact

# %%
email_df = mrkdrop.get_email_from_dropcontact(
    df[first_name_col], df[last_name_col], df[company_col], api_key, batch_size
)

# %%
email_df


# %% [markdown]
# # Write email_df to the same Google Sheet

# %%
# Fix phone number format.
def prepare_phone_number_for_sheets(phone_number):
    if phone_number != "":
        pattern = r'^'
        replacement = "'"
        return re.sub(pattern, replacement, phone_number)
    else:
        return phone_number

email_df['phone'] = email_df['phone'].apply(prepare_phone_number_for_sheets)

email_df


# %% run_control={"marked": true}
def df_to_gsheet(gsheet_name: str, df: pd.DataFrame) -> None:
    # Write to the sheet.
    # Make sure the sheet "email"(sheet_name) exists in the Google Sheet.
    sheet_name = "email"
    spread2 = gspread_pandas.Spread(
        gsheet_name, sheet=sheet_name, create_sheet=True, creds=creds
    )
    spread2.df_to_sheet(df, index=False)


#
df_to_gsheet(gsheet_name, email_df)
