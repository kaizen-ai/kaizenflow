# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python [conda env:venv] *
#     language: python
#     name: conda-env-venv-py
# ---

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2

import os

import core.config_builders as cfgb
import numpy as np 
import pandas as pd

# %%
# Initialize config.
config = cfgb.get_config_from_env()

# %% [markdown]
# # Execute

# %%
if config is None:
    raise ValueError("No config provided.")

# %%
if config["fail"]:
    raise ValueError("Fail.")
else:
    print("success")

# %%
