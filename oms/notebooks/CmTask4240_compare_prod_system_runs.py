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
# # Descriptions

# %% [markdown]
# The notebook compares the prod system outputs (PnL, DAG) when running with different values of `history_lookback`: 4 days vs 15 minutes.

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %%
import logging

import pandas as pd

import core.plotting as coplotti
import dataflow.core as dtfcore
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import reconciliation as reconcil

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Specify the paths to experiments

# %% [markdown]
# Currently we use `history_lookback = 4 days` in production, the candidate value of that is 15 minutes.

# %%
system_log_path_dict = {
    "prod": "/shared_data/history_lookback_experiment/system_log_dir_lookback_4_days",
    "candidate": "/shared_data/history_lookback_experiment/system_log_dir_lookback_15_minutes",
}
system_log_path_dict

# %%
# This dict points to `system_log_dir/dag/node_io/node_io.data` for different experiments.
data_type = "dag_data"
dag_path_dict = reconcil.get_system_log_paths(system_log_path_dict, data_type)
dag_path_dict

# %%
# This dict points to `system_log_dir/process_forecasts/portfolio` for different experiments.
data_type = "portfolio"
portfolio_path_dict = reconcil.get_system_log_paths(
    system_log_path_dict, data_type
)
portfolio_path_dict

# %% [markdown]
# # Compare DAG output

# %% [markdown]
# The DAG output should be the same taking into account the difference in history amount. A small experiment above confirms the previous statement.

# %%
# The last node, the last timestamp.
node_name = "predict.5.process_forecasts"
bar_timestamp = pd.Timestamp("2023-04-18 14:00:00-0400", tz="America/New_York")

# %%
dag_prod_df = dtfcore.get_dag_node_output(
    dag_path_dict["prod"], node_name, bar_timestamp
)
hpandas.df_to_str(dag_prod_df, num_rows=5, log_level=logging.INFO)

# %%
dag_candidate_df = dtfcore.get_dag_node_output(
    dag_path_dict["candidate"], node_name, bar_timestamp
)
hpandas.df_to_str(dag_candidate_df, num_rows=3, log_level=logging.INFO)

# %%
diff_threshold = 1e-3
compare_dfs_kwargs = {
    # Compare data only at intersecting indices as prod output has longer history.
    "row_mode": "inner",
    "diff_mode": "pct_change",
    "assert_diff_threshold": None,
}
diff_df = hpandas.compare_dfs(dag_prod_df, dag_candidate_df, **compare_dfs_kwargs)
max_diff = diff_df.abs().max().max()
max_diff

# %% [markdown]
# # Compare DAG execution time

# %% [markdown]
# By using shorter history lookback period we expect the execution time to drop. Indeed, the total execution time dropped ~3x.

# %%
df_dag_execution_time_prod = dtfcore.get_execution_time_for_all_dag_nodes(
    dag_path_dict["prod"]
)
_LOG.info("DAG execution time:")
hpandas.df_to_str(df_dag_execution_time_prod, num_rows=3, log_level=logging.INFO)

# %%
df_dag_execution_time_candidate = dtfcore.get_execution_time_for_all_dag_nodes(
    dag_path_dict["candidate"]
)
_LOG.info("DAG execution time:")
hpandas.df_to_str(
    df_dag_execution_time_candidate, num_rows=3, log_level=logging.INFO
)

# %%
dtfcore.plot_dag_execution_stats(df_dag_execution_time_prod, report_stats=True)

# %% [markdown]
# There are some outliers which is due to the fact that the system was run on the dev server.

# %% run_control={"marked": true}
dtfcore.plot_dag_execution_stats(
    df_dag_execution_time_candidate, report_stats=True
)

# %% [markdown]
# # Compare DAG memory consumption

# %% [markdown]
# DAG memory consumption is expected to drop. Indeed, the difference is 400x. Use a results df's size to measure memory consumption.

# %%
dag_df_out_size_prod = dtfcore.get_dag_df_out_size_for_all_nodes(
    dag_path_dict["prod"]
)
_LOG.info("DAG results df size:")
hpandas.df_to_str(dag_df_out_size_prod, num_rows=5, log_level=logging.INFO)

# %%
dag_df_out_size_candidate = dtfcore.get_dag_df_out_size_for_all_nodes(
    dag_path_dict["candidate"]
)
_LOG.info("DAG results df size:")
hpandas.df_to_str(dag_df_out_size_candidate, num_rows=5, log_level=logging.INFO)

# %%
# Display the results df size distribution over the DAG nodes.
dtfcore.plot_dag_df_out_size_stats(dag_df_out_size_prod, report_stats=False)

# %%
# Display the results df size distribution over the DAG nodes.
dtfcore.plot_dag_df_out_size_stats(dag_df_out_size_candidate, report_stats=False)

# %% [markdown]
# # Compare PnL

# %% [markdown]
# For C3a the amount of history should not affect the resulting PnL. Confirmed by comparing PnL plots.

# %%
bar_duration = "5T"
portfolio_dfs, portfolio_stats_dfs = reconcil.load_portfolio_dfs(
    portfolio_path_dict,
    bar_duration,
)
hpandas.df_to_str(portfolio_dfs["prod"], num_rows=5, log_level=logging.INFO)

# %%
portfolio_stats_df = pd.concat(portfolio_stats_dfs, axis=1)
bars_to_burn = 1
coplotti.plot_portfolio_stats(portfolio_stats_df.iloc[bars_to_burn:])
