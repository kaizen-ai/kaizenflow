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

import matplotlib.pyplot as plt
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
date_str = None
prod_subdir = None
config_list = oms.build_reconciliation_configs(date_str, prod_subdir)
config = config_list[0]
print(config)

# %% [markdown]
# # Specify data to load

# %% run_control={"marked": false}
# The dict points to `system_log_dir` for different experiments.
system_log_path_dict = dict(config["system_log_path"].to_dict())

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

# %% [markdown]
# # Compare configs (prod vs vim)

# %%
configs = oms.load_config_from_pickle(system_log_path_dict)
# Diff configs.
# TODO(Grisha): the output is only on subconfig level, we should
# compare value vs value instead.
diff_config = cconfig.build_config_diff_dataframe(
    {
        "prod_config": configs["prod"],
        "sim_config": configs["sim"],
    }
)
diff_config.T

# %% [markdown]
# # DAG io

# %% [markdown]
# ## Compare DAG io (prod vs sim)

# %%
# Get DAG node names.
dag_node_names = oms.get_dag_node_names(dag_path_dict["prod"])
_LOG.info(
    "The 1st node=%s, the last node=%s", dag_node_names[0], dag_node_names[-1]
)

# %%
# Get timestamps for the last DAG node.
dag_node_timestamps = oms.get_dag_node_timestamps(
    dag_path_dict["prod"], dag_node_names[-1], as_timestamp=True
)
_LOG.info(
    "The 1st timestamp=%s, the last timestamp=%s",
    dag_node_timestamps[0][0],
    dag_node_timestamps[-1][0],
)

# %%
# Load DAG output for different experiments.
dag_start_timestamp = None
dag_end_timestamp = None
dag_df_dict = oms.load_dag_outputs(
    dag_path_dict,
)
# Get DAG output for the last node and the last timestamp.
# TODO(Grisha): use 2 dicts -- one for the last node, last timestamp,
# the other one for all nodes, all timestamps for comparison.
dag_df_prod = dag_df_dict["prod"][dag_node_names[-1]][dag_node_timestamps[-1][0]]
dag_df_sim = dag_df_dict["sim"][dag_node_names[-1]][dag_node_timestamps[-1][0]]
hpandas.df_to_str(dag_df_prod, num_rows=5, log_level=logging.INFO)

# %%
# Compute difference.
compare_dfs_kwargs = {
    # TODO(Grisha): use `pct_change` once it is fixed for small numbers.
    "diff_mode": "diff",
    "remove_inf": True,
}
diff_df = hpandas.compare_multiindex_dfs(
    dag_df_prod,
    dag_df_sim,
    compare_dfs_kwargs=compare_dfs_kwargs,
)
# Remove the sign.
diff_df = diff_df.abs()
# Check that data is the same.
diff_df.max().max()

# %%
# Enable if the diff is big to see the detailed stats.
if False:
    # Plot over time.
    diff_df.max(axis=1).plot()
    plt.show()
    # Plot over column names.
    diff_df.max(axis=0).unstack().max(axis=1).plot(kind="bar")
    plt.show()
    # Plot over assets
    diff_df.max(axis=0).unstack().max(axis=0).plot(kind="bar")
    plt.show()

# %%
if False:
    # Compute correlations.
    prod_sim_dag_corr = dtfmod.compute_correlations(
        dag_df_prod,
        dag_df_sim,
    )
    hpandas.df_to_str(
        prod_sim_dag_corr.min(),
        num_rows=None,
        precision=3,
        log_level=logging.INFO,
    )

# %% [markdown]
# ## Compute DAG delay

# %%
delay_in_secs = oms.compute_dag_delay_in_seconds(dag_node_timestamps, display_plot=False)

# %% [markdown]
# # Portfolio

# %% [markdown]
# ## Compute research portfolio equivalent

# %%
# Set Portofolio start and end timestamps.
if True:
    # By default use the min/max bar timestamps from the DAG.
    start_timestamp = dag_node_timestamps[0][0]
    end_timestamp = dag_node_timestamps[-1][0]
else:
    # Overwrite if needed.
    start_timestamp = pd.Timestamp(
        "2022-11-03 06:05:00-04:00", tz="America/New_York"
    )
    end_timestamp = pd.Timestamp(
        "2022-11-03 08:00:00-04:00", tz="America/New_York"
    )
_LOG.info("start_timestamp=%s", start_timestamp)
_LOG.info("end_timestamp=%s", end_timestamp)

# %%
fep = dtfmod.ForecastEvaluatorFromPrices(
    **config["research_forecast_evaluator_from_prices"]["init"]
)
annotate_forecasts_kwargs = config["research_forecast_evaluator_from_prices"][
    "annotate_forecasts_kwargs"
].to_dict()
research_portfolio_df, research_portfolio_stats_df = fep.annotate_forecasts(
    dag_df_prod,
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
# ## Load logged portfolios (prod & sim)

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
hpandas.df_to_str(portfolio_dfs["prod"], num_rows=5, log_level=logging.INFO)

# %%
# Add research df and combine into a single df.
portfolio_stats_dfs["research"] = research_portfolio_stats_df
portfolio_stats_df = pd.concat(portfolio_stats_dfs, axis=1)
#
hpandas.df_to_str(portfolio_stats_df, num_rows=5, log_level=logging.INFO)

# %% [markdown]
# ## Compute Portfolio statistics (prod vs research vs sim)

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
# ## Compare portfolios pairwise (prod vs research vs sim)

# %% [markdown]
# ### Differences

# %% [markdown]
# #### Prod vs sim

# %%
# TODO(Grisha): @Dan factor out in a function.
# Compute difference.
compare_dfs_kwargs = {
    "column_mode": "inner",
    "diff_mode": "diff",
    "remove_inf": True,
    "assert_diff_threshold": None,
}
diff_df = hpandas.compare_multiindex_dfs(
    portfolio_dfs["prod"],
    portfolio_dfs["sim"],
    compare_dfs_kwargs=compare_dfs_kwargs,
)
# Remove the sign.
diff_df = diff_df.abs()
# Check that data is the same.
max_diff = diff_df.max().max()
_LOG.info("Max difference between prod and sim is=%s", max_diff)
prod_sim_diff = diff_df.max().unstack().max(axis=1).map("{:,.2f}".format)
hpandas.df_to_str(prod_sim_diff, num_rows=None, log_level=logging.INFO)

# %% [markdown]
# #### Prod vs research

# %%
diff_df = hpandas.compare_multiindex_dfs(
    portfolio_dfs["prod"],
    portfolio_dfs["research"],
    compare_dfs_kwargs=compare_dfs_kwargs,
)
# Remove the sign.
diff_df = diff_df.abs()
# Check that data is the same.
max_diff = diff_df.max().max()
_LOG.info("Max difference between prod and research is=%s", max_diff)
prod_research_diff = diff_df.max().unstack().max(axis=1).map("{:,.2f}".format)
hpandas.df_to_str(prod_research_diff, num_rows=None, log_level=logging.INFO)

# %% [markdown]
# #### Sim vs research

# %%
diff_df = hpandas.compare_multiindex_dfs(
    portfolio_dfs["sim"],
    portfolio_dfs["research"],
    compare_dfs_kwargs=compare_dfs_kwargs,
)
# Remove the sign.
diff_df = diff_df.abs()
# Check that data is the same.
max_diff = diff_df.max().max()
_LOG.info("Max difference between sim and research is=%s", max_diff)
sim_research_diff = diff_df.max().unstack().max(axis=1).map("{:,.2f}".format)
hpandas.df_to_str(sim_research_diff, num_rows=None, log_level=logging.INFO)

# %% [markdown]
# ### Correlations

# %% [markdown]
# #### Prod vs sim

# %%
if False:
    dtfmod.compute_correlations(
        portfolio_dfs["prod"],
        portfolio_dfs["sim"],
        allow_unequal_indices=False,
        allow_unequal_columns=False,
    )

# %% [markdown]
# #### Prod vs research

# %%
if False:
    dtfmod.compute_correlations(
        research_portfolio_df,
        portfolio_dfs["prod"],
        allow_unequal_indices=True,
        allow_unequal_columns=True,
    )

# %% [markdown]
# #### Sim vs research

# %%
if False:
    dtfmod.compute_correlations(
        research_portfolio_df,
        portfolio_dfs["sim"],
        allow_unequal_indices=True,
        allow_unequal_columns=True,
    )

# %% [markdown]
# # Target positions

# %% [markdown]
# ## Load target positions (prod)

# %%
prod_target_position_df = oms.load_target_positions(
    portfolio_path_dict["prod"].strip("portfolio"),
    start_timestamp,
    end_timestamp,
    config["meta"]["bar_duration"],
    normalize_bar_times=True
)
hpandas.df_to_str(prod_target_position_df, num_rows=5, log_level=logging.INFO)
if False:
    # TODO(Grisha): do we need to compare sim also?
    sim_target_position_df = oms.load_target_positions(
        portfolio_path_dict["sim"].strip("portfolio"),
        start_timestamp,
        end_timestamp,
        config["meta"]["bar_duration"],
        normalize_bar_times=True
    )

# %% [markdown]
# ## Compare positions target vs executed (prod)

# %%
# TODO(Grisha): decide what is the expected output.
df1 = prod_target_position_df["target_holdings_shares"].shift(1)
df2 = prod_target_position_df["holdings_shares"]
diff = df1 - df2
hpandas.df_to_str(diff, num_rows=5, log_level=logging.INFO)

# %% [markdown]
# ## Compare target positions (prod vs research)

# %% [markdown]
# ### Price

# %% run_control={"marked": true}
column = "price"
prod_df = prod_target_position_df[column]
display(prod_df.head(2))
res_df = research_portfolio_df[column]
display(res_df.head(2))

# Compute percentage difference.
diff_df = hpandas.compare_dfs(
    prod_df,
    res_df,
    diff_mode= "pct_change",
)
# Remove the sign and NaNs.
diff_df = diff_df.abs()
# Check that data is the same.
print(diff_df.max().max())
if False:
    hpandas.heatmap_df(diff_df.round(2))

# %% [markdown]
# ### Volatility

# %%
column = "volatility"
prod_df = prod_target_position_df[column]
display(prod_df.head(2))
res_df = research_portfolio_df[column]
display(res_df.head(2))

# Compute percentage difference.
diff_df = hpandas.compare_dfs(
    prod_df,
    res_df,
    diff_mode= "pct_change",
)
# Remove the sign and NaNs.
diff_df = diff_df.abs()
# Check that data is the same.
print(diff_df.max().max())
if False:
    hpandas.heatmap_df(diff_df.round(2))

# %% [markdown]
# ### Prediction

# %%
column = "prediction"
prod_df = prod_target_position_df[column]
display(prod_df.head(2))
res_df = research_portfolio_df[column]
display(res_df.head(2))

# Compute percentage difference.
diff_df = hpandas.compare_dfs(
    prod_df,
    res_df,
    diff_mode= "pct_change",
)
# Remove the sign and NaNs.
diff_df = diff_df.abs()
# Check that data is the same.
print(diff_df.max().max())
if False:
    hpandas.heatmap_df(diff_df.round(2))

# %% [markdown]
# ### Current holdings

# %%
column = "holdings_shares"
prod_df = prod_target_position_df[column]
display(prod_df.head(2))
res_df = research_portfolio_df[column]
display(res_df.head(2))

# Compute percentage difference.
diff_df = hpandas.compare_dfs(
    prod_df,
    res_df,
    diff_mode= "pct_change",
    assert_diff_threshold=None,
)
# Remove the sign and NaNs.
diff_df = diff_df.abs()
# Check that data is the same.
print(diff_df.max().max())
if False:
    hpandas.heatmap_df(diff_df.round(2))

# %% [markdown]
# ### Target holdings

# %%
prod_df = prod_target_position_df["target_holdings_shares"].shift(1)
display(prod_df.head(5))

res_df = research_portfolio_df["holdings_shares"]
display(res_df.head(5))

# Compute percentage difference.
diff_df = hpandas.compare_dfs(
    prod_df,
    res_df,
    diff_mode= "pct_change",
    assert_diff_threshold=None,
)
# Remove the sign and NaNs.
diff_df = diff_df.abs()
# Check that data is the same.
print(diff_df.max().max())
if False:
    hpandas.heatmap_df(diff_df.round(2))

# %% [markdown]
# # Orders 

# %% [markdown]
# ## Load orders (prod & sim)

# %%
prod_order_df = oms.TargetPositionAndOrderGenerator.load_orders(
    portfolio_path_dict["prod"].strip("portfolio"),
)
hpandas.df_to_str(prod_order_df, num_rows=5, log_level=logging.INFO)
sim_order_df = oms.TargetPositionAndOrderGenerator.load_orders(
    portfolio_path_dict["sim"].strip("portfolio"),
)
hpandas.df_to_str(sim_order_df, num_rows=5, log_level=logging.INFO)

# %% [markdown]
# ## Compare orders (prod vs sim)

# %%
# TODO(Grisha): add comparison using the usual `pct_change` approach.

# %% [markdown]
# # Fills statistics

# %%
# TODO(Grisha): what should we do here?
fills = oms.compute_fill_stats(prod_target_position_df)
hpandas.df_to_str(fills, num_rows=5, log_level=logging.INFO)
fills["underfill_share_count"].sum(axis=1).plot()

# %% [markdown]
# # Slippage

# %%
# TODO(Grisha): what should we do here?
slippage = oms.compute_share_prices_and_slippage(portfolio_dfs["prod"])
hpandas.df_to_str(slippage, num_rows=5, log_level=logging.INFO)
slippage["slippage_in_bps"].sum(axis=1).plot()

# %%
stacked = slippage[["slippage_in_bps", "is_benchmark_profitable"]].stack()
stacked[stacked["is_benchmark_profitable"] > 0]["slippage_in_bps"].hist(bins=31)
stacked[stacked["is_benchmark_profitable"] < 0]["slippage_in_bps"].hist(bins=31)

# %% [markdown]
# # Total cost accounting

# %%
notional_costs = oms.compute_notional_costs(
    portfolio_dfs["prod"],
    prod_target_position_df, 
)
hpandas.df_to_str(notional_costs, num_rows=5, log_level=logging.INFO)

# %%
cost_df = oms.apply_costs_to_baseline(
    portfolio_stats_dfs["research"],
    portfolio_stats_dfs["prod"],
    portfolio_dfs["prod"],
    prod_target_position_df, 
)
hpandas.df_to_str(cost_df, num_rows=5, log_level=logging.INFO)

# %%
cost_df[["pnl", "baseline_pnl_minus_costs", "baseline_pnl"]].plot()
cost_df[["pnl", "baseline_pnl_minus_costs"]].plot()

# %% [markdown]
# # TCA

# %%
if config["meta"]["run_tca"]:
    tca = cofinanc.load_and_normalize_tca_csv(tca_csv)
    tca = cofinanc.compute_tca_price_annotations(tca, True)
    tca = cofinanc.pivot_and_accumulate_holdings(tca, "")
