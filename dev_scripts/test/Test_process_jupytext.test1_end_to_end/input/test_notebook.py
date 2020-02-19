# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.1'
#       jupytext_version: 1.1.2
#   kernelspec:
#     display_name: Python [conda env:.conda-p1_develop] *
#     language: python
#     name: conda-env-.conda-p1_develop-py
# ---

# %% [markdown]
# ## Imports

# %%
# %load_ext autoreload
# %autoreload 2

import logging
import pprint

import tqdm.notebook as tqdm

import helpers.dbg as dbg
import helpers.env as env
import helpers.printing as prnt

# %%
print(env.get_system_signature()[0])

prnt.config_notebook()

dbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)
