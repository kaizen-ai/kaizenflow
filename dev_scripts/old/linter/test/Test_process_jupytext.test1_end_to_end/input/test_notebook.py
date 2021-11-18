# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.1'
#       jupytext_version: 1.1.2
#   kernelspec:
#     display_name: Python [conda env:.conda-develop] *
#     language: python
#     name: conda-env-.conda-develop-py
# ---

# %% [markdown]
# ## Imports

# %%
# %load_ext autoreload
# %autoreload 2

import logging

import helpers.dbg as hdbg
import helpers.env as henv
import helpers.printing as hprint

# %%
print(henv.get_system_signature()[0])

hprint.config_notebook()

hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)
