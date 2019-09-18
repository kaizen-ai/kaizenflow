# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.2'
#       jupytext_version: 1.2.1
#   kernelspec:
#     display_name: Python [conda env:develop] *
#     language: python
#     name: conda-env-develop-py
# ---

# %% [markdown]
# ## Imports

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

import numpy as np
import pandas as pd

import core.explore as exp

# %% [markdown]
# ## exp.display_df

# %%
np.random.seed(100)

x = 5 * np.random.randn(100)
y = x + np.random.randn(*x.shape)
df = pd.DataFrame()
df["x"] = x
df["y"] = y

exp.display_df(df)

# %% [markdown]
# ## exp.ols_regress_series

# %%
np.random.seed(100)

x = 5 * np.random.randn(100)
y = x + np.random.randn(*x.shape)
df = pd.DataFrame()
df["x"] = x
df["y"] = y

exp.ols_regress_series(df["x"], df["y"], intercept=True)

# %%
