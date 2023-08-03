# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Description
#
# This gallery notebook is used to verify that `amp/core/plotting` functions display plots correctly.

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %% [markdown]
# # Imports

# %% run_control={"marked": false}
import logging

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import core.plotting.misc_plotting as cplmiplo
import core.plotting.visual_stationarity_test as cpvistte
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint

# %% [markdown]
# # Configure Logger

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Plots

# %% [markdown]
# ## `plot_histograms_and_lagged_scatterplot()`

# %%
# Set inputs.
seq = np.concatenate(
    [np.random.uniform(-1, 1, 100), np.random.choice([5, 10], 100)]
)
index = pd.date_range(start="2023-01-01", periods=len(seq), freq="D")
srs = pd.Series(seq, index=index)
lag = 7
# TODO(Dan): Remove after integration with `cmamp`
figsize = (20, 20)
# Plot.
cpvistte.plot_histograms_and_lagged_scatterplot(srs, lag, figsize=figsize)

# %% [markdown]
# ## `plot_projection()`

# %%
# Calling plot_projection to test that it's basic functionality works.
data = [
    [1, 1, 0, 1],
    [0, 1, 0, 1],
    [0, 0, 1, 1],
    [1, 1, 1, 1],
    [1, 1, 1, 1],
]
index = pd.date_range(start="2023-01-01", periods=len(data), freq="D")
df = pd.DataFrame(data, index=index)
fig = plt.figure()
ax = fig.add_axes([0, 0, 1, 1])
cplmiplo.plot_projection(df, special_values=[0], ax=ax)

# %%
# Calling plot_projection with the scatter plot mode enabled.
fig = plt.figure()
ax = fig.add_axes([0, 0, 1, 1])
cplmiplo.plot_projection(df, special_values=[0], mode="scatter", ax=ax)

# %%
