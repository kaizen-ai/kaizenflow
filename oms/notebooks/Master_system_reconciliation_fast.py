# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Description

# %% [markdown]
# The notebook contains checks that compare the prod system output vs that of the prod simulation run ensuring basic sanity.

# %% [markdown]
# # Imports

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
import core.statistics as costatis
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
# Get config from env when running the notebook via the `run_notebook.py` script, e.g.,
# in the system reconciliation flow.
config = cconfig.get_config_from_env()
if config:
    _LOG.info("Using config from env vars")
else:
    _LOG.info("Using hardwired config")
    # Specify the config directly when running the notebook manually.
    # Below is just an example.
    dst_root_dir = "/shared_data/ecs/preprod/prod_reconciliation/"
    dag_builder_name = "C3a"
    start_timestamp_as_str = "20230802_154500"
    end_timestamp_as_str = "20230802_160000"
    run_mode = "prod"
    mode = "scheduled"
    config_list = oms.build_reconciliation_configs(
        dst_root_dir,
        dag_builder_name,
        start_timestamp_as_str,
        end_timestamp_as_str,
        run_mode,
        mode,
    )
    config = config_list[0]
print(config)

# %% [markdown]
# # Specify data to load

# %% run_control={"marked": false}
# The dict points to `system_log_dir` for different experiments.
system_log_path_dict = dict(config["system_log_path"].to_dict())

# %%
# This dict points to `system_log_dir/dag/node_io/node_io.data` for different experiments.
data_type = "dag_data"
dag_path_dict = oms.get_system_log_paths(system_log_path_dict, data_type)
dag_path_dict

# %%
# This dict points to `system_log_dir/process_forecasts/portfolio` for different experiments.
data_type = "portfolio"
portfolio_path_dict = oms.get_system_log_paths(system_log_path_dict, data_type)
portfolio_path_dict

# %%
# This dict points to `system_log_dir/process_forecasts/orders` for different experiments.
data_type = "orders"
orders_path_dict = oms.get_system_log_paths(system_log_path_dict, data_type)
orders_path_dict

# %%
# TODO(gp): Load the TCA data for crypto.
if config["meta"]["run_tca"]:
    tca_csv = os.path.join(
        root_dir, config["meta"]["date_str"], "tca/sau1_tca.csv"
    )
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
# ## Load

# %%
# Get DAG node names.
dag_node_names = oms.get_dag_node_names(dag_path_dict["prod"])
_LOG.info(
    "First node='%s' / Last node='%s'", dag_node_names[0], dag_node_names[-1]
)

# %%
# Get timestamps for the last DAG node.
dag_node_timestamps = oms.get_dag_node_timestamps(
    dag_path_dict["prod"], dag_node_names[-1], as_timestamp=True
)
_LOG.info(
    "First timestamp='%s'/ Last timestamp='%s'",
    dag_node_timestamps[0][0],
    dag_node_timestamps[-1][0],
)

# %%
# Get DAG output for the last node and the last timestamp.
dag_df_prod = oms.load_dag_outputs(dag_path_dict["prod"], dag_node_names[-1])
dag_df_sim = oms.load_dag_outputs(dag_path_dict["sim"], dag_node_names[-1])
_LOG.info("Output of the last node:\n")
hpandas.df_to_str(dag_df_prod, num_rows=5, log_level=logging.INFO)

# %% [markdown]
# ## Check DAG io self-consistency

# %%
# TODO(Grisha): pass the value via config.
diff_threshold = 1e-3
compare_dfs_kwargs = {
    # TODO(Nina): CmTask4387 "DAG self-consistency check fails when
    # `history_lookback=15T` for C3a".
    "compare_nans": False,
    "diff_mode": "pct_change",
    "assert_diff_threshold": None,
}

# %%
# Run for the last timestamp only as a sanity check.
bar_timestamp = dag_node_timestamps[-1][0]
_LOG.info("Running the DAG self-consistency check for=%s", bar_timestamp)
# Compare DAG output at T with itself at time T-1.
oms.check_dag_output_self_consistency(
    dag_path_dict["prod"],
    dag_node_names[-1],
    bar_timestamp,
    trading_freq=config["meta"]["bar_duration"],
    diff_threshold=diff_threshold,
    **compare_dfs_kwargs,
)

# %% [markdown]
# ## Compare DAG io (prod vs sim)

# %%
# Run for the last node and the last timestamp only as a sanity check.
bar_timestamp = dag_node_timestamps[-1][0]
node_name = dag_node_names[-1]
_LOG.info(
    "Comparing DAG output for node=%s and bar_timestamp=%s",
    node_name,
    bar_timestamp,
)
oms.compare_dag_outputs(
    dag_path_dict,
    node_name,
    bar_timestamp,
    diff_threshold=diff_threshold,
    **compare_dfs_kwargs,
)

# %%
# TODO(Grisha): Functions do not work currently. Think of another approach to compute stats.
# The functions assumes that it is possible to keep all the DAG output in memory which is
# not always true.
if False:
    dag_diff_df = oms.compute_dag_outputs_diff(dag_df_dict, **compare_dfs_kwargs)
    dag_diff_detailed_stats = oms.compute_dag_output_diff_detailed_stats(
        dag_diff_df
    )

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
# ## Compute DAG execution time

# %%
df_dag_execution_time = oms.get_execution_time_for_all_dag_nodes(
    dag_path_dict["prod"]
)
_LOG.info("DAG execution time:")
hpandas.df_to_str(df_dag_execution_time, num_rows=5, log_level=logging.INFO)

# %%
oms.plot_dag_execution_stats(df_dag_execution_time, report_stats=True)

# %%
# The time is an approximation of how long it takes to process a bar. Technically the time
# is a distance (in secs) between wall clock time when an order is executed and a bar
# timestamp. The assumption is that order execution is the very last stage.
df_order_execution_time = oms.get_orders_execution_time(orders_path_dict["prod"])
# TODO(Grisha): consider adding an assertion that checks that the time does not
# exceed one minute.
_LOG.info(
    "Max order execution time=%s secs",
    df_order_execution_time["execution_time"].max(),
)

# %% [markdown]
# # Compute DAG memory consumption

# %%
# Use a results df's size to measure memory consumption.
dag_df_out_size = oms.get_dag_df_out_size_for_all_nodes(dag_path_dict["prod"])
_LOG.info("DAG results df size:")
hpandas.df_to_str(dag_df_out_size, num_rows=5, log_level=logging.INFO)

# %%
# Display the results df size distribution over the DAG nodes.
oms.plot_dag_df_out_size_stats(dag_df_out_size, report_stats=False)

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
portfolio_dfs, portfolio_stats_dfs = oms.load_portfolio_dfs(
    portfolio_path_dict,
    config["meta"]["bar_duration"],
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
report_stats = False
display_plot = False
compare_dfs_kwargs = {
    "column_mode": "inner",
    "diff_mode": "pct_change",
    "remove_inf": True,
    "assert_diff_threshold": None,
}
portfolio_diff_df = oms.compare_portfolios(
    portfolio_dfs,
    report_stats=report_stats,
    display_plot=display_plot,
    **compare_dfs_kwargs,
)
hpandas.df_to_str(portfolio_diff_df, num_rows=None, log_level=logging.INFO)

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
    config["meta"]["bar_duration"],
)
hpandas.df_to_str(prod_target_position_df, num_rows=5, log_level=logging.INFO)
if False:
    # TODO(Grisha): compare prod vs sim at some point.
    sim_target_position_df = oms.load_target_positions(
        portfolio_path_dict["sim"].strip("portfolio"),
        config["meta"]["bar_duration"],
    )

# %% [markdown]
# ## Compare positions target vs executed (prod)

# %%
# TODO(Grisha): use `hpandas.compare_dfs()`.
df1 = prod_target_position_df["target_holdings_shares"].shift(1)
df2 = prod_target_position_df["holdings_shares"]
diff = df1 - df2
hpandas.df_to_str(diff, num_rows=5, log_level=logging.INFO)

# %% [markdown]
# ## Compare target positions (prod vs research)

# %% [markdown]
# ### Price

# %% run_control={"marked": true}
# TODO(Grisha): wrap in a function since it's common for all columns.
column = "price"
prod_df = prod_target_position_df[column]
res_df = research_portfolio_df[column]

# Compute percentage difference.
diff_df = hpandas.compare_dfs(
    prod_df,
    res_df,
    diff_mode="pct_change",
    # Some pipelines (e.g., "C8b") do not use "close" prices to compute
    # "twap" / "vwap". While when computing target positions "close"
    # prices from MarketData are used. Thus the perfect match is not
    # expected.
    assert_diff_threshold=None,
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
res_df = research_portfolio_df[column]

# Compute percentage difference.
diff_df = hpandas.compare_dfs(
    prod_df,
    res_df,
    diff_mode="pct_change",
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
res_df = research_portfolio_df[column]

# Compute percentage difference.
diff_df = hpandas.compare_dfs(
    prod_df,
    res_df,
    diff_mode="pct_change",
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
res_df = research_portfolio_df[column]

# Compute percentage difference.
diff_df = hpandas.compare_dfs(
    prod_df,
    res_df,
    diff_mode="pct_change",
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
res_df = research_portfolio_df["holdings_shares"]

# Compute percentage difference.
diff_df = hpandas.compare_dfs(
    prod_df,
    res_df,
    diff_mode="pct_change",
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
fills = oms.compute_fill_stats(prod_target_position_df)
hpandas.df_to_str(fills, num_rows=5, log_level=logging.INFO)

# %%
col = "fill_rate"
coplotti.plot_boxplot(fills[col], "by_row", ylabel=col)

# %%
col = "fill_rate"
coplotti.plot_boxplot(fills[col], "by_col", ylabel=col)

# %% [markdown]
# # Slippage

# %%
slippage = oms.compute_share_prices_and_slippage(portfolio_dfs["prod"])
hpandas.df_to_str(slippage, num_rows=5, log_level=logging.INFO)

# %%
col = "slippage_in_bps"
coplotti.plot_boxplot(slippage[col], "by_row", ylabel=col)

# %%
col = "slippage_in_bps"
coplotti.plot_boxplot(slippage[col], "by_col", ylabel=col)

# %%
stacked = slippage[["slippage_in_bps", "is_benchmark_profitable"]].stack()
slippage_when_benchmark_profitable = stacked[
    stacked["is_benchmark_profitable"] > 0
]["slippage_in_bps"].rename("slippage_when_benchmark_profitable")
slippage_when_not_benchmark_profitable = stacked[
    stacked["is_benchmark_profitable"] <= 0
]["slippage_in_bps"].rename("slippage_when_not_benchmark_profitable")
slippage_when_benchmark_profitable.hist(bins=31, edgecolor="black", color="green")
slippage_when_not_benchmark_profitable.hist(
    bins=31, alpha=0.7, edgecolor="black", color="red"
)

# %%
slippage_benchmark_profitability_ecdfs = (
    costatis.compute_and_combine_empirical_cdfs(
        [
            slippage_when_benchmark_profitable,
            slippage_when_not_benchmark_profitable,
        ]
    )
)
slippage_benchmark_profitability_ecdfs.plot(yticks=np.arange(0, 1.1, 0.1))

# %% [markdown]
# # Total cost accounting

# %%
notional_costs = oms.compute_notional_costs(
    portfolio_dfs["prod"],
    prod_target_position_df,
)
hpandas.df_to_str(notional_costs, num_rows=5, log_level=logging.INFO)

# %%
oms.summarize_notional_costs(notional_costs, "by_bar").plot(kind="bar")

# %%
oms.summarize_notional_costs(notional_costs, "by_asset").plot(kind="bar")

# %%
oms.summarize_notional_costs(notional_costs, "by_bar").sum()

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

# %%
