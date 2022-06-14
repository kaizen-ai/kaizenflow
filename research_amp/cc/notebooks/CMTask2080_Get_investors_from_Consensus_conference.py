# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.8
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Imports

# %%
import pandas as pd

# %% [markdown]
# # Load the data

# %%
data = pd.read_csv("/shared_data/consensus_conference.csv", index_col = "Unnamed: 0")
data

# %% [markdown]
# # Tier 1 filter

# %%
# `Partner` in job, `Venture,Capital,VC` in company.
tier1 = pd.concat([
    data[data["job"].str.contains('Partner', case = False, na=False)],
    data[data["company"].str.contains('venture|capital|VC', case = False, na=False)],
])
# Exclude LLP (lawyer firms).
tier1 = tier1[~tier1["company"].str.contains('LLP', na=False)]
tier1 = tier1.drop_duplicates()
tier1

# %%
# `Phantombuster` format (name + company)
tier1_phantombuster = tier1['name'].map(str) + ' ' + tier1['company'].map(str)
tier1_phantombuster
