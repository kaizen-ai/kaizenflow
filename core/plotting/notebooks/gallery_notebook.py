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

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import core.plotting.visual_stationarity_test as cpvistte
import core.plotting.misc_plotting as cpmiscplt

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
seq = np.concatenate([np.random.uniform(-1, 1, 100), np.random.choice([5, 10], 100)])
index = pd.date_range(start="2023-01-01", periods=len(seq), freq="D")
srs = pd.Series(seq, index=index)
lag = 7
# TODO(Dan): Remove after integration with `cmamp`
figsize = (20,20)
# Plot.
cpvistte.plot_histograms_and_lagged_scatterplot(srs, lag, figsize=figsize)

# %%
# Create large dataframe of random ints between 0 and 100.
import pdb
rand_df = pd.DataFrame(np.random.randint(0,100,size=(300, 6)), columns=['y1','y2','y3','y4','y5','y6'])
#fig, ax = plt.subplots()
#ax.plot(range(0, 100), range(0, 100))
fig = plt.figure()
ax = fig.add_axes([0,0,1,1])
pdb.set_trace()
cpmiscplt.plot_projection(rand_df, special_values = [1000], mode = "scatter", ax = ax)

# %%
range_df_2 = rand_df.copy()
for i in range(rand_df.shape[1]):
    range_df_2.iloc[:, i] = i
range_df_2.head(50)

# %%
