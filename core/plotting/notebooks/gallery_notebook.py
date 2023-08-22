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

import core.config as cconfig
import core.plotting.correlation as cplocorr
import core.plotting.misc_plotting as cplmiplo
import core.plotting.test.test_plots as cptetepl
import core.plotting.visual_stationarity_test as cpvistte
import dataflow.model.model_plotter as dtfmomoplo
import dataflow.model.test.test_model_evaluator as cdmttme
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Build config

# %%
config = cconfig.get_config_from_env()
if config:
    _LOG.info("Using config from env vars")
else:
    _LOG.info("Using hardwired config")
    config_dict = {"figsize": (20, 10)}
    config = cconfig.Config.from_dict(config_dict)
print(config)

# %% [markdown]
# # Plots

# %% [markdown]
# ## `plot_histograms_and_lagged_scatterplot()`

# %%
test_srs = cptetepl.Test_plots.get_plot_histograms_and_lagged_scatterplot1()
lag = 7
# %%
cpvistte.plot_histograms_and_lagged_scatterplot(
    test_srs, lag, figsize=config["figsize"]
)

# %% [markdown]
# ## `plot_timeseries_distribution()`
# %%
test_srs = cptetepl.Test_plots.get_plot_timeseries_distribution1()

# %%
datetime_types = ["hour"]
cplmiplo.plot_timeseries_distribution(test_srs, datetime_types)

# %%
datetime_types = ["hour", "month"]
cplmiplo.plot_timeseries_distribution(test_srs, datetime_types)

# %% [markdown]
# ## `plot_time_series_by_period()`

# %%
test_srs = cptetepl.Test_plots.get_plot_time_series_by_period1()

# %%
period = "day"
cplmiplo.plot_time_series_by_period(test_srs, period)

# %%
period = "time"
cplmiplo.plot_time_series_by_period(test_srs, period)

# %% [markdown]
# ## `plot_heatmap()`

# %%
mode = "clustermap"
test_df = cptetepl.Test_plots.get_test_plot_df1()

# %%
cplocorr.plot_heatmap(test_df, mode, figsize=config["figsize"])

# %% [markdown]
# ## `plot_performance()`

# %%
evaluator, eval_config = cdmttme.get_example_model_evaluator()
plotter = dtfmomoplo.ModelPlotter(evaluator)
keys = None


# %%
plotter.plot_performance(
    keys=keys,
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
    target_volatility=eval_config["target_volatility"],
)

# %% [markdown]
# ## `plot_rets_signal_analysis()`

# %%
evaluator, eval_config = cdmttme.get_example_model_evaluator()
plotter = dtfmomoplo.ModelPlotter(evaluator)
keys = None


# %%
plotter.plot_performance(
    keys=keys,
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
    target_volatility=eval_config["target_volatility"],
)

# %% [markdown]
# ## `plot_effective_correlation_rank()`

# %%
test_df = cptetepl.Test_plots.get_test_plot_df1()

# %%
cplocorr.plot_effective_correlation_rank(test_df)

# %%
num_q_values = 5
q_values = np.random.uniform(1, 10, num_q_values).tolist()
cplocorr.plot_effective_correlation_rank(test_df, q_values)

# %% [markdown]
# ## `plot_spectrum()`

# %%
test_df = cptetepl.Test_plots.get_test_plot_df1()

# %%
cplmiplo.plot_spectrum(test_df)

# %%
_, axes = plt.subplots(2, 2, figsize=config["figsize"])
axes_flat = axes.flatten()
cplmiplo.plot_spectrum(signal=test_df, axes=axes_flat)

# %%

# %%
