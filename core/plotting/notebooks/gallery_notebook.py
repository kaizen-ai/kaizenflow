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
import core.plotting.test.test_plots as cptetepl
import core.plotting.visual_stationarity_test as cpvistte
import core.plotting.test.test_plots as cptetepl
import core.plotting.correlation as cplocorr

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint

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
srs = cptetepl.Test_plots.get_plot_histograms_and_lagged_scatterplot1()
lag = 7
# TODO(Dan): Remove after integration with `cmamp`. Changes from Cm #4722 are not in `sorrentum` yet.
figsize = (20, 20)
cpvistte.plot_histograms_and_lagged_scatterplot(srs, lag, figsize=figsize)
# %% [markdown]
# ## `plot_timeseries_distribution()`
# %%
# Set inputs for hour interval.
cptetepl.Test_plots.setUpClass()
srs = cptetepl.Test_plots.get_plot_timeseries_distribution1()
datetime_types = ["hour"]
cplmiplo.plot_timeseries_distribution(srs, datetime_types)

# %%
# Set input for hour and month interval.
srs = cptetepl.Test_plots.get_plot_timeseries_distribution1()
datetime_types = ["hour", "month"]
cplmiplo.plot_timeseries_distribution(srs, datetime_types)

# %% [markdown]
# ## `plot_time_series_by_period()`

# %%
# Set inputs.
test_series = cptetepl.Test_plots.get_plot_time_series_by_period1()

# %%
period = "day"
cplmiplo.plot_time_series_by_period(test_series, period)

# %%
period = "time"
cplmiplo.plot_time_series_by_period(test_series, period)

# %% [markdown]
# ## `plot_heatmap()`

# %%
mode = "clustermap"
corr_df = cptetepl.Test_plots.get_plot_heatmap()
figsize = (20, 20)
cplocorr.plot_heatmap(corr_df, mode, figsize=figsize)

# %% [markdown]
# ## `plot_effective_correlation_rank()`

# %%
# Set inputs.
test_df = cptetepl.Test_plots.get_plot_effective_correlation_rank1()

# %%
cplocorr.plot_effective_correlation_rank(test_df)

# %%
num_q_values = 5 
q_values = np.random.uniform(1, 10, num_q_values).tolist()
cplocorr.plot_effective_correlation_rank(test_df, q_values)

# %% [markdown]
# ## `plot_spectrum()`

# %%
# Set inputs.
test_df = cptetepl.Test_plots.get_plot_spectrum1()

# %%
cplmiplo.plot_spectrum(test_df)

# %%
figsize = (20, 20)
_, axes = plt.subplots(2, 2, figsize=figsize)
axes_flat = axes.flatten()
cplmiplo.plot_spectrum(signal=test_df, axes=axes_flat)

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
