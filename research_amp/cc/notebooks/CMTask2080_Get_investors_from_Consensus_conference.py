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
# `Partner` in job.
mask1 = data["job"].str.contains('Partner', case = False, na=False)
print("Selecting partner=", mask1.sum())
# `Venture,Capital,VC` in company.
mask2 = data["company"].str.contains('venture|capital|VC', case = False, na=False)
print("Company count=", mask2.sum())
# Exclude LLP (lawyer firms).
mask3 = data["company"].str.contains('LLP', na=False)
print("Company count=", mask3.sum())

# Collect all Tier-1 contacts.
tier1 = data[(mask1|mask2) & ~mask3]
tier1 = tier1.drop_duplicates()
tier1

# %%
# `Phantombuster` format (name + company)
tier1_phantombuster = tier1['name'].map(str) + ' ' + tier1['company'].map(str)
tier1_phantombuster
