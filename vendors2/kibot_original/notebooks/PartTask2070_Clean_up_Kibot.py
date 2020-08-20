# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.4.1
#   kernelspec:
#     display_name: Python [conda env:.conda-p1_develop] *
#     language: python
#     name: conda-env-.conda-p1_develop-py
# ---

# %% [markdown]
# ## Description

# %% [markdown]
# Exploratory notebook for sanity-checking Kibot metadata

# %% [markdown]
# ## Imports

# %%
# %load_ext autoreload
# %autoreload 2

import logging


import helpers.dbg as dbg
import helpers.env as env
import helpers.printing as prnt
import vendors.kibot.utils as kut

# %%
dbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", env.get_system_signature()[0])

prnt.config_notebook()

# %% [markdown]
# ## Load Kibot

# %% [markdown]
# ### Continuous contract metadata

# %%
# Load with the standalone function.
continuous_contract_metadata = kut.read_continuous_contract_metadata()

# %%
# Look at the data.
#
# There are problems:
#   - The last row is all NaNs
#   - The index is float instead of int (b/c the last row has a NaN value)
#   - The `StartDate` column is not a datetime column
#   - (harder) The Symbols (seems "SymbolBase" always equals "Symbol")
#     does not always match the CME GLOBEX symbol even when the "Exchange"
#     description includes "GLOBEX"
continuous_contract_metadata

# %%
# Same exercise but with the class
km = kut.KibotMetadata()

# %%
km_metadata = km.get_metadata()

# %%
# Potential data issues:
#   - km_metadata["Description"].str.contains("CONTINUOUS").value_counts()
#     shows 250 contracts with the name "CONTINUOUS" in the them, yet the
#     standalone function returns 87. Why?
#
#   - "StartDate" is not a datetime column
#   - "min_contract" and "max_contract" should also be datetime columns
#   - "num_contracts" and "num_expiries" should be "int"
km_metadata

# %%
