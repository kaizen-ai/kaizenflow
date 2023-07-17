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

import core.plotting.misc_plotting as cplmiplo
import core.plotting.test.test_plots as cptetepl
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
# ## `plot_time_series_by_period()`

# %%
# Set inputs.
test_series1 = cptetepl.Test_plots.get_plot_time_series_by_period1()

# %%
# Set inputs.
test_series2 = cptetepl.Test_plots.get_plot_time_series_by_period2()

# %%
period = "day"
cplmiplo.plot_time_series_by_period(test_series1, period)

# %%
period = "time"
cplmiplo.plot_time_series_by_period(test_series2, period)

# %%
