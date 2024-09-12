# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Description

# %% [markdown]
# Display the backtest results as the output of the `ForecastEvaluator`, created in `Master_research_backtest_analyzer` notebook.
#
# The user provides a list of the outputs of the 2nd stage of the backtest, and the notebook displays the portfolio stats for the provided runs.

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %%
import logging

import pandas as pd

import core.config as cconfig
import core.plotting as coplotti
import dataflow.model as dtfmod
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Build the config dict

# %%
# Get config from env when running the notebook via the `run_notebook.py` script.
default_config = cconfig.get_config_from_env()
if default_config:
    _LOG.info("Using config from env vars")
else:
    _LOG.info("Using hardwired config")
    default_config_dict = {
        # Provide a list of experiment output dirs for analysis.
        "system_log_dirs": [
            "/shared_data/backtest.C14a.config1/build_tile_configs.C14a.ccxt_v8_1-all.15T.2023-08-01_2024-07-07.ins.run0/portfolio_dfs/20240708_125433/default_config"
        ],
        "pnl_resampling_frequency": "D",
        "bin_annotated_portfolio_df_kwargs": {
            "proportion_of_data_per_bin": 0.2,
            "normalize_prediction_col_values": False,
        },
        # Start date to trim the data for analysis.
        # To use all data, use "None".
        # Example of `start_date` value:
        # "start_date": pd.Timestamp("2023-08-01", tz="US/Eastern")
        "start_date": None,
    }
    # Build config from dict.
    default_config = cconfig.Config().from_dict(default_config_dict)
print(default_config)

# %% [markdown]
# # Load portfolio metrics

# %%
# Load the portfolio DFs and stats.
bar_metrics_dict = {}
portfolio_df_dict = {}
start_date = default_config["start_date"]
for index, system_log_dir in enumerate(default_config["system_log_dirs"]):
    (
        portfolio_df,
        bar_metrics,
    ) = dtfmod.AbstractForecastEvaluator.load_portfolio_and_stats(system_log_dir)
    bar_metrics_dict[index] = bar_metrics.loc[start_date:]
    portfolio_df_dict[index] = portfolio_df.loc[start_date:]
    # Trim to given start_date, if provided.
    if start_date is not None:
        _LOG.warning("Trimming data starting from %s", str(start_date))
        bar_metrics = bar_metrics.loc[start_date:]
        portfolio_df = portfolio_df.loc[start_date:]
    bar_metrics_dict[index] = bar_metrics
    portfolio_df_dict[index] = portfolio_df
portfolio_stats_df = pd.concat(bar_metrics_dict, axis=1)

# %% [markdown]
# # Portfolio stats

# %%
coplotti.plot_portfolio_stats(
    portfolio_stats_df, freq=default_config["pnl_resampling_frequency"]
)

# %%
# Plot binned stats.
coplotti.plot_portfolio_binned_stats(
    portfolio_df_dict,
    **default_config["bin_annotated_portfolio_df_kwargs"],
)

# %% [markdown]
# # Aggregate portfolio stats

# %%
stats_computer = dtfmod.StatsComputer()

# %%
portfolio_stats, daily_metrics = stats_computer.compute_portfolio_stats(
    portfolio_stats_df,
    default_config["pnl_resampling_frequency"],
)
display(portfolio_stats)

# %%
