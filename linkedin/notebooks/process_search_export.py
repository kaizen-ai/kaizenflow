# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Imports

# %%
# !sudo /bin/bash -c "(source /venv/bin/activate; pip install gspread_pandas)"

# %%
import gspread_pandas
import logging
import helpers.hdbg as hdbg
import linkedin.google_api.google_file_api as gapi

# %%
_LOG = logging.getLogger(__name__)
hdbg.init_logger(use_exec_path=True)

# %% [markdown]
# # Load data

# %%
spreadsheet_name = "sn_search5.search_export.gsheet"
spread = gspread_pandas.Spread(spreadsheet_name)
df = spread.sheet_to_df(index=None)
print(df.shape)
df.head()

# %% [markdown]
# # Filter data

# %%
# Drop errors.
prev_len = len(df)
df = df[df["error"] == ""].reset_index(drop=True)
df = df[[col for col in df.columns if col != "error"]]
print(
    f"Dropped {prev_len - len(df)} rows ({round((prev_len - len(df))*100/prev_len, 2)}%)"
)
df.head()

# %%
# Filter by location.
# The location you want to keep.
location = "Maryland"
prev_len = len(df)
df = df[df["location"].apply(lambda x: location in x)].reset_index(drop=True)
print(
    f"Dropped {prev_len - len(df)} rows ({round((prev_len - len(df))*100/prev_len, 2)}%)"
)
df.head()

# %%
# Make sure profileUrl in the first column.
cols = df.columns.tolist()
if cols[0] != 'profileUrl':
    cols.remove('profileUrl')  # Remove the "profileURL" from its current position.
    cols = ['profileUrl'] + cols  # Add "profileURL" at the first column.
    df = df[cols] 
df.head()

# %%
print(df.shape)
df

# %% [markdown]
# # Save filtered data to gsheet

# %%
# Set parameters.
# gfile_type: 'sheet' or 'doc'.
gfile_type = 'sheet'
gfile_name = 'new_google_sheet'
folder_name = ''
folder_id = ''
user = ''

# %%
gapi.create_empty_google_file(
    gfile_type=gfile_type,
    gfile_name = gfile_name,
    folder_name = folder_name,
    folder_id = folder_id,
    user = user
)

# %%
# A Google sheet with this name should already exist on the drive.
new_spreadsheet_name = "sn_search5.search_export.filtered.gsheet"
spread2 = gspread_pandas.Spread(
    new_spreadsheet_name,
    sheet="Sheet1",
    create_sheet=True,
)
spread2.df_to_sheet(df, index=False)

# %%
