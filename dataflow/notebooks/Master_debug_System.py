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
# # Description

# %% [markdown]
# The goal of this notebook is to demonstrate the various approaches of working with `dataflow`.

# %% [markdown]
# # Imports

# %%
import logging
import os

import pandas as pd

import core.config.config_ as cconconf
import core.finance as cofinanc
import core.finance.resampling as cfinresa
import core.finance.returns as cfinretu
import dataflow.core as dtfcore
import dataflow.system as dtfsys
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import im_v2.ccxt.data.client as icdcl

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()

# %%
dag_node_io_dir = "../../../node_io"

file_name = dag_node_io_dir + "/predict.0.read_data.df_out.csv"

df = pd.read_csv(file_name, index_col=0, header=[0, 1])

df.head()

# %%
df.tail()

# %%
df.iloc[-1]

# %%
# !ls ../../../node_io
