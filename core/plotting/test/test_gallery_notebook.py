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
# This notebook will be used to plot all charts from amp/core/plotting in order to verify that the plots are being displayed correctly.

# %% [markdown]
# ## Plot Histograms and Lagged Scatterplots

# %% run_control={"marked": false}
import numpy as np
import pandas as pd

import core.plotting.visual_stationarity_test as cpvistte


# %%
srs = pd.Series(
    np.concatenate([np.random.uniform(-1, 1, 100), np.random.choice([5, 10], 100)]),
    index=pd.date_range(start="2023-01-01", periods=200, freq="D")
)
lag = 7

# %%
cpvistte.plot_histograms_and_lagged_scatterplot(srs, lag, figsize = (20,20))

# %%
