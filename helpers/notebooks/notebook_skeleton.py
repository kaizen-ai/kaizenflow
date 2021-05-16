# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.0
#   kernelspec:
#     display_name: Python [conda env:venv] *
#     language: python
#     name: conda-env-venv-py
# ---

# %% [markdown]
# # Description
#
# This notebook examines ...

# %%
# %load_ext autoreload
# %autoreload 2

import logging

import helpers.dbg as dbg
import helpers.env as env
import helpers.printing as prnt

# %%
print(env.get_system_signature()[0])

prnt.config_notebook()

# %%
# dbg.init_logger(verbosity=logging.DEBUG)
dbg.init_logger(verbosity=logging.INFO)
# dbg.test_logger()
_LOG = logging.getLogger(__name__)
