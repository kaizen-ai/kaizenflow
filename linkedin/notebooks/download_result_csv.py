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

# %%
import linkedin.phantom_api.phantombuster_api as ph

# %%
# Get all phantoms and their phantom id.
phantom = ph.Phantom()
phantom.get_all_phantoms()

# %%
# (INPUT) Set your search name and phantom id (Choose id from the above table).
search_name = "sn_search5"
search_phantom_id = "2862499141527492"
profile_phantom_id = "3593602419926765"

# %%
# Path to save result csv.
result_dir = "../result_csv/"
search_result_csv_path = result_dir + f"{search_name}_search_result.csv"
profile_result_csv_path = result_dir + f"{search_name}_profile_result.csv"

# %%
# Download search result csv.
phantom.download_result_csv_by_phantom_id(search_phantom_id, search_result_csv_path)

# %%
# Download profile result csv.
phantom.download_result_csv_by_phantom_id(profile_phantom_id, profile_result_csv_path)
