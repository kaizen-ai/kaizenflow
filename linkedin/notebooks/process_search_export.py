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
# !sudo /bin/bash -c "(source /venv/bin/activate; pip install google-api-python-client)"

# %%
import gspread_pandas

# %%
import logging
import helpers.hdbg as hdbg
import linkedin.google_api.google_file_api as google_file_api

# %%
_LOG = logging.getLogger(__name__)
hdbg.init_logger(use_exec_path=True)

# %% [markdown]
# # Load data

# %%
spreadsheet_name = "1_search_export_sns5"
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

# %% [markdown]
# # Save filtered data to gsheet

# %% [markdown]
# ## Create a empty Google sheet

# %%
gapi = google_file_api.GoogleFileApi()

# %%
name = 'SN_Search5_Yiyun'
# gdrive_folder : dict, the id and the name of the Google Drive folder.
gdrive_folder  = gapi.get_folder_id_by_name(name)

# %%
# if you want to use another folder id, please change the folder id manually.
# gdrive_folder  = {'id': '1XWNGDnJrVICHAe-6V2cnoSklZpk0APc_', 'name': 'SN_Search5_Yiyun'} 

# %%
"""
Create a new Google file (sheet or doc).

:param gfile_type: str, the type of the Google file ('sheet' or 'doc').
:param gfile_name: str, the name of the new Google file.
:param folder_id: str, the id of the Google Drive folder.
:param user: str, the email address of the user to share the Google file (Optional).
:return: None
"""
gfile_type = 'sheet'
gsheet_name = '2_search_export_filtered_sns5'
user = ''

# %%
gapi.create_empty_google_file(
    gfile_type = gfile_type,
    gfile_name = gsheet_name,
    gdrive_folder = gdrive_folder,
    user = user
)

# %% [markdown]
# ## Save filtered data

# %%
# A Google sheet with this name should already exist on the drive.
spread2 = gspread_pandas.Spread(
    gsheet_name,
    sheet="Sheet1",
    create_sheet=True,
)
spread2.df_to_sheet(df, index=False)

# %%
