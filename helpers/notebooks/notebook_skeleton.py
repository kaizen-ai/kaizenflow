# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.12.0
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

import helpers.dbg as hdbg
import helpers.env as henv
import helpers.printing as hprint

# %%
print(henv.get_system_signature()[0])

hprint.config_notebook()

# %%
# hdbg.init_logger(verbosity=logging.DEBUG)
hdbg.init_logger(verbosity=logging.INFO)
# hdbg.test_logger()
_LOG = logging.getLogger(__name__)
