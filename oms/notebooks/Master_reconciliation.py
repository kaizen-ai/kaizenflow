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
import pprint

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
import helpers.hsql as hsql
import im_v2.ccxt.data.client as icdcl
import im_v2.common.universe as ivcu
import im_v2.im_lib_tasks as imvimlita
import oms as oms

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Config

# %% [markdown]
# ## Build reconciliation config

# %%
date_str = "20221028"
config_list = oms.build_reconciliation_configs(date_str)
config = config_list[0]
print(config)

# %% [markdown]
# ## Specify paths

# %% run_control={"marked": true}
# Point to `system_log_dir` for different experiments.
system_log_path_dict = dict(config["system_log_path"].to_dict())
print("# system_log_path_dict=\n%s" % pprint.pformat(system_log_path_dict))

# Point to `system_log_dir/process_forecasts/portfolio` for different experiments.
data_type = "portfolio"
portfolio_path_dict = oms.get_system_log_paths(system_log_path_dict, data_type, verbose=True)
#print("\n# portfolio_path_dict=\n%s" % pprint.pformat(portfolio_path_dict))

# Point to `system_log_dir/dag/node_io/node_io.data` for different experiments.
data_type = "dag"
dag_path_dict = oms.get_system_log_paths(system_log_path_dict, data_type, verbose=True)

# %%
date_str = config["meta"]["date_str"]
# TODO(gp): @Grisha infer this from the data from prod Portfolio df, but allow to overwrite.
start_timestamp = pd.Timestamp(date_str + " 06:05:00", tz="America/New_York")
_LOG.info("start_timestamp=%s", start_timestamp)
end_timestamp = pd.Timestamp(date_str + " 07:50:00", tz="America/New_York")
_LOG.info("end_timestamp=%s", end_timestamp)


# %% [markdown]
# # Compare configs

# %%
configs = oms.load_config_from_pickle(system_log_path_dict)
# Diff configs.
# TODO(gp): @grisha let's add the keys as column names.
diff_config = cconfig.build_config_diff_dataframe(
    {
        "prod_config": configs["prod"],
        "sim_config": configs["sim"],
    }
)
diff_config.T

# %%
# TODO(gp): Load the TCA data for crypto.
if config["meta"]["run_tca"]:
    tca_csv = os.path.join(root_dir, date_str, "tca/sau1_tca.csv")
    hdbg.dassert_file_exists(tca_csv)

# %%
# file_name1 = "/shared_data/prod_reconciliation/20221025/prod/system_log_dir_scheduled__2022-10-24T10:00:00+00:00_2hours/dag/node_io/node_io.data/predict.8.process_forecasts.df_out.20221025_061000.parquet"
# df1 = pd.read_parquet(file_name1)

# file_name2 = "/shared_data/prod_reconciliation/20221025/simulation/system_log_dir/dag/node_io/node_io.data/predict.8.process_forecasts.df_out.20221025_061000.parquet"
# df2 = pd.read_parquet(file_name2)

# %%
#df1.columns.levels[0]

# %%
if False:
    #asset_id = 1030828978
    asset_id = 9872743573
    #df1['vwap.ret_0.vol_adj_2_hat', asset_id] == df2['vwap.ret_0.vol_adj_2_hat', asset_id]
    #column = 'vwap.ret_0.vol_adj_2_hat'
    column = 'close'

    #pd.concat()

    compare_visually_dataframes_kwargs = {"diff_mode": "pct_change", "background_gradient": False}
    subset_multiindex_df_kwargs = {"columns_level0": [column],
                                   #"columns_level1": [asset_id]
                                  }

    hpandas.compare_multiindex_dfs(df1, df2,
                                   subset_multiindex_df_kwargs=subset_multiindex_df_kwargs,
                                   compare_visually_dataframes_kwargs=compare_visually_dataframes_kwargs )#.dropna().abs().max()

# %% [markdown]
# # Data delay analysis

# %%
# Get the real-time `ImClient`.
# TODO(Grisha): ideally we should get the values from the config.
resample_1min = False
env_file = imvimlita.get_db_env_path("dev")
connection_params = hsql.get_connection_info_from_env_file(env_file)
db_connection = hsql.get_connection(*connection_params)
table_name = "ccxt_ohlcv_futures"
#
im_client = icdcl.CcxtSqlRealTimeImClient(
    resample_1min, db_connection, table_name
)

# %%
# Get the universe.
# TODO(Grisha): get the version from the config.
vendor = "CCXT"
mode = "trade"
version = "v7.1"
as_full_symbol = True
full_symbols = ivcu.get_vendor_universe(
    vendor,
    mode,
    version=version,
    as_full_symbol=as_full_symbol,
)
full_symbols

# %%
# Load the data for the reconciliation date.
# `ImClient` operates in UTC timezone.
start_ts = pd.Timestamp(date_str, tz="UTC")
end_ts = start_ts + pd.Timedelta(days=1)
columns = None
filter_data_mode = "assert"
df = im_client.read_data(
    full_symbols, start_ts, end_ts, columns, filter_data_mode
)
hpandas.df_to_str(df, num_rows=5, log_level=logging.INFO)

# %%
# TODO(Grisha): move to a lib.
# Compute delay in seconds.
df["delta"] = (df["knowledge_timestamp"] - df.index).dt.total_seconds()
# Plot the delay over assets with the errors bars.
minimums = df.groupby(by=["full_symbol"]).min()["delta"]
maximums = df.groupby(by=["full_symbol"]).max()["delta"]
means = df.groupby(by=["full_symbol"]).mean()["delta"]
errors = [means - minimums, maximums - means]
df.groupby(by=["full_symbol"]).mean()["delta"].sort_values(ascending=False).plot(
    kind="bar", yerr=errors
)

# %% [markdown]
# # Compare DAG io

# %%
# Get DAG node names.
dag_node_names = oms.get_dag_node_names(dag_path_dict["prod"])
print(hprint.to_str("dag_node_names"))

dag_node_name = dag_node_names[-1]
print(hprint.to_str("dag_node_name"))

# %%
# Get timestamps for the target DAG node.
dag_node_timestamps = oms.get_dag_node_timestamps(
    dag_path_dict["prod"], dag_node_name, as_timestamp=True
)
print("dag_node_timestamps=\n%s" % hprint.indent("\n".join(map(str, dag_node_timestamps))))

dag_node_timestamp = dag_node_timestamps[-1]
print("dag_node_timestamp=%s" % dag_node_timestamp)

# %%
# Load DAG output for different experiments.
dag_df_dict = {}
for experiment_name, path in dag_path_dict.items():
    print(hprint.to_str("experiment_name"))
    # Get DAG node names for every experiment.
    dag_nodes = oms.get_dag_node_names(path)
    # Get timestamps for the last node.
    dag_node_ts = oms.get_dag_node_timestamps(
        path, dag_node_name, as_timestamp=True
    )
    # Get DAG output for the last node and the last timestamp.
    dag_df_dict[experiment_name] = oms.get_dag_node_output(
        path, dag_node_name, dag_node_timestamp,
    )
    hpandas.df_to_str(dag_df_dict[experiment_name], num_rows=5, log_level=logging.INFO)


# %%
# Trim the data to match the target interval.
for k, df in dag_df_dict.items():
    dag_df_dict[k] = dag_df_dict[k].loc[start_timestamp:end_timestamp]

hpandas.df_to_str(dag_df_dict["prod"], num_rows=5, log_level=logging.INFO)

# %%
dag_df_dict["prod"]

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
# Build the ForecastEvaluator.
forecast_evaluator_from_prices_kwargs = config["research_forecast_evaluator_from_prices"]["init"]
print(hprint.to_str("forecast_evaluator_from_prices_kwargs", mode="pprint"))
fep = dtfmod.ForecastEvaluatorFromPrices(**forecast_evaluator_from_prices_kwargs)
# Run.
annotate_forecasts_kwargs = config["research_forecast_evaluator_from_prices"][
    "annotate_forecasts_kwargs"
]
print(hprint.to_str("annotate_forecasts_kwargs", mode="pprint"))
research_portfolio_df, research_portfolio_stats_df = fep.annotate_forecasts(
    dag_df_dict["prod"],
    **annotate_forecasts_kwargs.to_dict(),
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
hpandas.df_to_str(portfolio_stats_df[["prod", "research"]], num_rows=5, log_level=logging.INFO)

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
# # Load forecast dataframes

# %%
prod_target_position_df = oms.load_target_positions(
    portfolio_path_dict["prod"].strip("portfolio"),
    start_timestamp,
    end_timestamp,
    config["meta"]["bar_duration"],
    normalize_bar_times=True
)

cand_target_position_df = oms.load_target_positions(
    portfolio_path_dict["cand"].strip("portfolio"),
    start_timestamp,
    end_timestamp,
    config["meta"]["bar_duration"],
    normalize_bar_times=True
)

# %%
#prod_target_position_df["target_holdings_shares"].shift(1)#
research_portfolio_df["holdings_shares"]

# %%
research_portfolio_df["holdings_shares"].head(10)

# %%
#prod_target_position_df.columns.levels[0]
prod_target_position_df["holdings_shares"].head(10)

# %%
research_portfolio_df["holdings_shares"].head(10)

# %% [markdown]
# # Orders

# %%
prod_order_df = oms.TargetPositionAndOrderGenerator.load_orders(
    portfolio_path_dict["prod"].strip("portfolio"),
)
hpandas.df_to_str(prod_order_df, log_level=logging.INFO)

# %%
sim_order_df = oms.TargetPositionAndOrderGenerator.load_orders(
    portfolio_path_dict["sim"].strip("portfolio"),
)
hpandas.df_to_str(sim_order_df, log_level=logging.INFO)

# %%
prod_order_df.groupby(["creation_timestamp", "asset_id"]).count()

# %%
asset_id = 6051632686

mask = prod_order_df["asset_id"] == asset_id
prod_order_df[mask].head(6)

# %%
prod_target_position_df["target_trades_shares"][asset_id].head(20)

# %%
# We are getting the fills that correspond to the orders and to the change of holdings.
prod_target_position_df["holdings_shares"][asset_id].diff()

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
