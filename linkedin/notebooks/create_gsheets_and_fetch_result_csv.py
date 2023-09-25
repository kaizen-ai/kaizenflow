# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Import

# %% run_control={"marked": true}
# Before running this notebook, please read the instruction here:
# https://gspread-pandas.readthedocs.io/en/latest/getting_started.html#client-credentials
# Follow the steps in `Client Credentials` until you have the JSON file downloaded. 
# Save that JSON as `client_secrets.json` and put it in `../config/` folder, then you are all set.

# !sudo /bin/bash -c "(source /venv/bin/activate; pip install --upgrade google-auth google-auth-httplib2 google-auth-oauthlib google-api-python-client)"
# !sudo /bin/bash -c "(source /venv/bin/activate; pip install gspread-pandas)"

# %%
import logging
import helpers.hdbg as hdbg
import helpers.hio as hio
import linkedin.phantom_api.phantombuster_api as lpphapia
import helpers.hgoogle_file_api as hgofiapi

# %%
_LOG = logging.getLogger(__name__)
hdbg.init_logger(use_exec_path=True)

# %% [markdown]
# # Initial

# %%
phantom = lpphapia.Phantom()

# %% [markdown]
# # Input

# %%
# (INPUT)Set the search name, it will also be the folder name, 
# or set it as '' to create files in your Google Drive root folder.
search_name = "sn_search5_test"

# %%
# (INPUT)Set the parent folder: your new folder will be created in this folder.
# "1dQ9e-bNKkXwNvobQyRFbPwgEh1-VSf4R" is linkedin_data folder id.
# In the URL address: https://drive.google.com/drive/u/0/folders/1dQ9e-bNKkXwNvobQyRFbPwgEh1-VSf4R
# 1dQ9e-bNKkXwNvobQyRFbPwgEh1-VSf4R is folder id.
parent_folder_id = "1dQ9e-bNKkXwNvobQyRFbPwgEh1-VSf4R"

# %%
# Set gsheets name.
gsheets_name = [
    f"{search_name}.step1.search_export",
    f"{search_name}.step2.search_export_filtered",
    f"{search_name}.step3.profile_export",
    f"{search_name}.step3.search_export_filtered",
]

# %%
# Get all phantoms and their phantom id.
phantom.get_all_phantoms()

# %%
# (INPUT) Set the phantom IDs (Choose ID from the above table).
search_phantom_id = "2862499141527492"
profile_phantom_id = "3593602419926765"

# %%
# Path to save result csv.
result_dir = "../result_csv/"
search_result_csv_path = result_dir + f"{search_name}_search_result.csv"
profile_result_csv_path = result_dir + f"{search_name}_profile_result.csv"

# %% [markdown]
# # Create the empty Google Drive folder and Google sheets

# %%
# Create a folder with search_name in the dir parent folder.
current_folder_id = hgofiapi.create_google_drive_folder(search_name, parent_folder_id)

# %%
# Create empty gsheets in the new created folder.
for gsheet_name in gsheets_name:
    hgofiapi.create_empty_google_file(
        gfile_type = "sheet",
        gfile_name = gsheet_name,
        gdrive_folder_id = current_folder_id,
        user = ""
    )

# %% [markdown]
# # Download result CSVs to local storage

# %%
# Download search result csv.
phantom.download_result_csv_by_phantom_id(search_phantom_id, search_result_csv_path)

# %%
# Download profile result csv.
phantom.download_result_csv_by_phantom_id(profile_phantom_id, profile_result_csv_path)

# %% [markdown]
# # Upload result CSVs to Google sheets

# %%
import gspread_pandas
import pandas as pd

# %%
search_export_df = pd.read_csv(search_result_csv_path)
profile_export_df = pd.read_csv(profile_result_csv_path)

# %%
search_export_df.head()

# %%
profile_export_df.head()


# %%
def df_to_gsheet(gsheet_name: str, df: pd.DataFrame) -> None:
    creds = hgofiapi.get_credentials()
    gsheet = gspread_pandas.Spread(
        gsheet_name,
        create_sheet=True,
        creds=creds
    )
    gsheet.df_to_sheet(df, index=False)
    _LOG.info("Save to gsheet %s", gsheet_name)


# %%
df_to_gsheet(f"{search_name}.step1.search_export", search_export_df)
df_to_gsheet(f"{search_name}.step3.profile_export", profile_export_df)

# %% [markdown]
# # Delete temp result CSVs

# %%
hio.delete_file(search_result_csv_path)
_LOG.info("Delete file %s", search_result_csv_path)

# %%
hio.delete_file(profile_result_csv_path)
_LOG.info("Delete file %s", profile_result_csv_path)

# %%
