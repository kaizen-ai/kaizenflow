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
# This notebook was used for prototyping ways to postprocess docsend data that has been exported to a csv in GSheets.

# %%
#imports 

import sys
sys.path.append('../../helpers')

from hgoogle_file_api import get_credentials, get_gdrive_service
#import helpers.google_file_api as hgofiapi

# %%

credentials = get_credentials()
gdrive_service = get_gdrive_service()


