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

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %%
import logging
import os

import numpy as np
import pandas as pd

import core.config as cconfig
import core.finance as cofinanc
import core.plotting as coplotti
import dataflow.model as dtfmod
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import oms as oms

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Build the reconciliation config

# %%
config_list = oms.build_reconciliation_configs()
config = config_list[0]
print(config)

# %% [markdown]
# # Specify data to load

# %% run_control={"marked": true}
# The dict points to `system_log_dir` for different experiments.
system_log_path_dict = dict(config["system_log_path"].to_dict())

# %%
configs = oms.load_config_from_pickle(system_log_path_dict)
# Diff configs.
diff_config = cconfig.build_config_diff_dataframe(
    {
        "prod_config": configs["prod"],
        "sim_config": configs["sim"],
    }
)
diff_config

# %%
# This dict points to `system_log_dir/process_forecasts/portfolio` for different experiments.
data_type = "portfolio"
portfolio_path_dict = oms.get_system_log_paths(system_log_path_dict, data_type)
portfolio_path_dict

# %%
# This dict points to `system_log_dir/dag/node_io/node_io.data` for different experiments.
data_type = "dag"
dag_path_dict = oms.get_system_log_paths(system_log_path_dict, data_type)
dag_path_dict

# %%
# TODO(gp): Load the TCA data for crypto.
if config["meta"]["run_tca"]:
    tca_csv = os.path.join(root_dir, date_str, "tca/sau1_tca.csv")
    hdbg.dassert_file_exists(tca_csv)

# %%
date_str = config["meta"]["date_str"]
# TODO(gp): @Grisha infer this from the data from prod Portfolio df, but allow to overwrite.
start_timestamp = pd.Timestamp(date_str + " 06:05:00", tz="America/New_York")
_LOG.info("start_timestamp=%s", start_timestamp)
end_timestamp = pd.Timestamp(date_str + " 08:00:00", tz="America/New_York")
_LOG.info("end_timestamp=%s", end_timestamp)


# %% [markdown]
# # Compare DAG io

# %%
# Get DAG node names.
dag_node_names = oms.get_dag_node_names(dag_path_dict["prod"])
dag_node_names

# %%
# Get timestamps for the last DAG node.
dag_node_timestamps = oms.get_dag_node_timestamps(
    dag_path_dict["prod"], dag_node_names[-1], as_timestamp=True
)
dag_node_timestamps

# %%
# Load DAG output for different experiments.
dag_df_dict = {}
for name, path in dag_path_dict.items():
    # Get DAG node names for every experiment.
    dag_nodes = oms.get_dag_node_names(path)
    # Get timestamps for the last node.
    dag_node_ts = oms.get_dag_node_timestamps(
        path, dag_nodes[-1], as_timestamp=True
    )
    # Get DAG output for the last node and the last timestamp.
    dag_df_dict[name] = oms.get_dag_node_output(
        path, dag_nodes[-1], dag_node_ts[-1]
    )
hpandas.df_to_str(dag_df_dict["prod"], num_rows=5, log_level=logging.INFO)

# %%
# Compute percentage difference.
compare_visually_dataframes_kwargs = {
    "diff_mode": "pct_change",
    "background_gradient": False,
}
diff_df = hpandas.compare_multiindex_dfs(
    dag_df_dict["prod"],
    dag_df_dict["sim"],
    compare_visually_dataframes_kwargs=compare_visually_dataframes_kwargs,
)
# Remove the sign and NaNs.
diff_df = diff_df.replace([np.inf, -np.inf], np.nan).abs()
# Check that data is the same.
diff_df.max().max()

# %%
# Plot diffs over time.
diff_df.max(axis=1).plot()

# %%
# Plot diffs over columns.
diff_df.max(axis=0).unstack().max(axis=1).plot(kind="bar")

# %%
# Plot diffs over assets.
diff_df.max(axis=0).unstack().max(axis=0).plot(kind="bar")

# %%
# Compute correlations.
prod_sim_dag_corr = dtfmod.compute_correlations(
    dag_df_dict["prod"],
    dag_df_dict["sim"],
)
hpandas.df_to_str(
    prod_sim_dag_corr.min(),
    num_rows=None,
    precision=3,
    log_level=logging.INFO,
)

# %%
# Make sure they are exactly the same.
(dag_df_dict["prod"] - dag_df_dict["sim"]).abs().max().max()

# %% [markdown]
# # Compute research portfolio equivalent

# %%
fep = dtfmod.ForecastEvaluatorFromPrices(
    **config["research_forecast_evaluator_from_prices"]["init"]
)
annotate_forecasts_kwargs = config["research_forecast_evaluator_from_prices"][
    "annotate_forecasts_kwargs"
].to_dict()
research_portfolio_df, research_portfolio_stats_df = fep.annotate_forecasts(
    dag_df_dict["prod"],
    **annotate_forecasts_kwargs,
    compute_extended_stats=True,
)
# TODO(gp): Move it to annotate_forecasts?
research_portfolio_df = research_portfolio_df.sort_index(axis=1)
# Align index with prod and sim portfolios.
research_portfolio_df = research_portfolio_df.loc[start_timestamp:end_timestamp]
research_portfolio_stats_df = research_portfolio_stats_df.loc[
    start_timestamp:end_timestamp
]
#
hpandas.df_to_str(research_portfolio_stats_df, num_rows=5, log_level=logging.INFO)

# %% [markdown]
# # Load logged portfolios

# %%
portfolio_config = cconfig.Config.from_dict(
    {
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "freq": config["meta"]["bar_duration"],
        "normalize_bar_times": True,
    }
)
portfolio_config

# %%
portfolio_dfs, portfolio_stats_dfs = oms.load_portfolio_dfs(
    portfolio_path_dict,
    portfolio_config,
)
# Add research portfolio.
portfolio_dfs["research"] = research_portfolio_df
#
hpandas.df_to_str(portfolio_dfs["prod"], num_rows=5, log_level=logging.INFO)

# %%
# Add research df and combine into a single df.
portfolio_stats_dfs["research"] = research_portfolio_stats_df
portfolio_stats_df = pd.concat(portfolio_stats_dfs, axis=1)
#
hpandas.df_to_str(portfolio_stats_df, num_rows=5, log_level=logging.INFO)

# %%
bars_to_burn = 1
coplotti.plot_portfolio_stats(portfolio_stats_df.iloc[bars_to_burn:])

# %%
stats_computer = dtfmod.StatsComputer()
stats_sxs, _ = stats_computer.compute_portfolio_stats(
    portfolio_stats_df.iloc[bars_to_burn:], config["meta"]["bar_duration"]
)
display(stats_sxs)

# %% [markdown]
# # Compare pairwise portfolio correlations

# %%
dtfmod.compute_correlations(
    research_portfolio_df,
    portfolio_dfs["prod"],
    allow_unequal_indices=True,
    allow_unequal_columns=True,
)

# %%
dtfmod.compute_correlations(
    portfolio_dfs["prod"],
    portfolio_dfs["sim"],
    allow_unequal_indices=False,
    allow_unequal_columns=False,
)

# %%
dtfmod.compute_correlations(
    research_portfolio_df,
    portfolio_dfs["sim"],
    allow_unequal_indices=True,
    allow_unequal_columns=True,
)

# %%
if config["meta"]["run_tca"]:
    tca = cofinanc.load_and_normalize_tca_csv(tca_csv)
    tca = cofinanc.compute_tca_price_annotations(tca, True)
    tca = cofinanc.pivot_and_accumulate_holdings(tca, "")
