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

# %%
#date_str = "20221028"
#date_str = "20221031"
date_str = "20221101"
prod_subdir = "system_log_dir_manual__2022-11-01T12:39:45.395761+00:00_2hours"
config_list = oms.build_reconciliation_configs(date_str, prod_subdir)
config = config_list[0]
print(config)

# %% run_control={"marked": true}
system_log_path_dict, portfolio_path_dict, dag_path_dict = oms.get_path_dicts(config, log_level=logging.INFO)

# %%
date_str = config["meta"]["date_str"]
# TODO(gp): @Grisha infer this from the data from prod Portfolio df, but allow to overwrite.
if False:
    start_time = "06:05:00"
    end_time = "07:50:00"
else:
    start_time = "08:45:00"
    end_time = "10:40:00"
start_timestamp = pd.Timestamp(date_str + " " + start_time, tz="America/New_York")
end_timestamp = pd.Timestamp(date_str + " " + end_time, tz="America/New_York")
_LOG.info("start_timestamp=%s", start_timestamp)
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

# %% [markdown]
# ## One-off reading of Parquet files

# %%
if False:
    file_name1 = "/shared_data/prod_reconciliation/20221025/prod/system_log_dir_scheduled__2022-10-24T10:00:00+00:00_2hours/dag/node_io/node_io.data/predict.8.process_forecasts.df_out.20221025_061000.parquet"
    df1 = pd.read_parquet(file_name1)

    file_name2 = "/shared_data/prod_reconciliation/20221025/simulation/system_log_dir/dag/node_io/node_io.data/predict.8.process_forecasts.df_out.20221025_061000.parquet"
    df2 = pd.read_parquet(file_name2)

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
# ## Read last node

# %%
# Select a specific node and timestamp to analyze.
log_level = logging.DEBUG
dag_node_names = oms.get_dag_node_names(dag_path_dict["prod"], log_level=log_level)
dag_node_name = dag_node_names[-1]
print(hprint.to_str("dag_node_name"))

dag_node_timestamps = oms.get_dag_node_timestamps(
    dag_path_dict["prod"], dag_node_name, as_timestamp=True, log_level=log_level
)

dag_node_timestamp = dag_node_timestamps[-1]
print("dag_node_timestamp=%s" % dag_node_timestamp)

# %%
# Load DAG output for different experiments.
dag_df_dict = oms.load_dag_outputs(dag_path_dict, dag_node_name, dag_node_timestamp, 
                                   start_timestamp, end_timestamp,
                                   log_level=logging.INFO)

# %%
# Load specific timestamp and node.
if False:
    # Load DAG output for different experiments.
    dag_node_timestamp = pd.Timestamp("2022-10-28 06:25:00-04:00")

    # dag_node_timestamps = oms.get_dag_node_timestamps(
    #     dag_path_dict["prod"], dag_node_name, as_timestamp=True, log_level=log_level
    # )
    # print(dag_node_timestamps)

    dag_df_dict = oms.load_dag_outputs(dag_path_dict, dag_node_name, dag_node_timestamp, 
                                       start_timestamp, end_timestamp,
                                       log_level=logging.INFO)

    # Check last two rows.
    #column = "vwap.ret_0.vol_adj_2_hat"
    column = "twap.ret_0"
    #display(dag_df_dict["prod"][column].tail(2))
    #display(dag_df_dict["sim"][column].tail(2))

    diff_df = hpandas.compare_visually_dataframes(
        dag_df_dict["prod"][column].tail(2),
        dag_df_dict["sim"][column].tail(2),
        diff_mode="pct_change",
        background_gradient=False)

    display(diff_df)

    #print(type(diff_df))
    diff_df.max().max()

# %%
#hpandas.compare_visually_dataframes(dag_df_dict["prod"], dag_df_dict["sim"])

# %%
_ = hpandas.multiindex_df_info(dag_df_dict["prod"])

# %%
df_subset = hpandas.subset_multiindex_df(dag_df_dict["prod"], columns_level0="close")
df_subset.head(2)

# %% [markdown]
# ## Compare prod vs sim DAG output

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
display(diff_df)
# # Remove the sign and NaNs.
# diff_df = diff_df.replace([np.inf, -np.inf], np.nan).abs()
# display(diff_df)
# # Check that data is the same.
# diff_df.max().max()

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
portfolio_config.to_dict()

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

sim_target_position_df = oms.load_target_positions(
    portfolio_path_dict["sim"].strip("portfolio"),
    start_timestamp,
    end_timestamp,
    config["meta"]["bar_duration"],
    normalize_bar_times=True
)

# %%
print("# prod_target_pos_df")
_ = hpandas.multiindex_df_info(prod_target_position_df, max_num=None)

print("\n# research_portfolio_df")
_ = hpandas.multiindex_df_info(research_portfolio_df, max_num=None)

# %%
column = "prediction"
prod_df = prod_target_position_df[column]
display(prod_df.head(2))
res_df = research_portfolio_df[column]
display(res_df.head(2))

# Compute percentage difference.
compare_visually_dataframes_kwargs = {
    "diff_mode": "pct_change",
    "background_gradient": False,
}
diff_df = hpandas.compare_visually_dataframes(
    prod_df,
    res_df,
    **compare_visually_dataframes_kwargs,
)
# Remove the sign and NaNs.
diff_df = diff_df.replace([np.inf, -np.inf], np.nan).abs()
# Check that data is the same.
print(diff_df.max().max())
hpandas.heatmap_df(diff_df.round(2))

# %% [markdown]
# ## Price

# %%
prod_df = prod_target_position_df["price"]
display(prod_df.head(3))
res_df = research_portfolio_df["price"]
display(res_df.head(3))

# Compute percentage difference.
compare_visually_dataframes_kwargs = {
    "diff_mode": "pct_change",
    "background_gradient": False,
    "log_level": logging.INFO,
}
diff_df = hpandas.compare_visually_dataframes(
    prod_df,
    res_df,
    **compare_visually_dataframes_kwargs,
)
# # Remove the sign and NaNs.
# diff_df = diff_df.replace([np.inf, -np.inf], np.nan).abs()
# # Check that data is the same.
# print(diff_df.max().max())
display(hpandas.heatmap_df(diff_df.round(2)))

# %% [markdown]
# ## Volatility

# %%
prod_df = prod_target_position_df["volatility"]
display(prod_df.head(2))
res_df = research_portfolio_df["volatility"]
display(res_df.head(2))

# Compute percentage difference.
compare_visually_dataframes_kwargs = {
    "diff_mode": "pct_change",
    "background_gradient": False,
    "log_level": logging.INFO,
}
diff_df = hpandas.compare_visually_dataframes(
    prod_df,
    res_df,
    **compare_visually_dataframes_kwargs,
)
# # Remove the sign and NaNs.
# diff_df = diff_df.replace([np.inf, -np.inf], np.nan).abs()
# # Check that data is the same.
# print(diff_df.max().max())
display(hpandas.heatmap_df(diff_df.round(2)))

# %% [markdown]
# ## Target holdings

# %%
prod_df = prod_target_position_df["target_holdings_shares"].shift(1)
display(prod_df.head(5))

res_df = research_portfolio_df["holdings_shares"]
display(res_df.head(5))

# Compute percentage difference.
compare_visually_dataframes_kwargs = {
    "diff_mode": "pct_change",
    "background_gradient": False,
    "log_level": logging.INFO,
}
diff_df = hpandas.compare_visually_dataframes(
    prod_df,
    res_df,
    **compare_visually_dataframes_kwargs,
)
# # Remove the sign and NaNs.
# diff_df = diff_df.replace([np.inf, -np.inf], np.nan).abs()
# # Check that data is the same.
# #diff_df.max().max()
display(hpandas.heatmap_df(diff_df.round(2)))

# %% [markdown]
# ## Holdings

# %%
display(prod_target_position_df["target_holdings_shares"].head(5))

display(research_portfolio_df["holdings_shares"].head(5))

# %%
prod_df = prod_target_position_df["holdings_shares"]
display(prod_df.head(5))

res_df = research_portfolio_df["holdings_shares"]
display(res_df.head(5))

# Compute percentage difference.
compare_visually_dataframes_kwargs = {
    "diff_mode": "pct_change",
    "background_gradient": False,
    "log_level": logging.INFO,
    "assert_diff_threshold": None,
}
diff_df = hpandas.compare_visually_dataframes(
    prod_df,
    res_df,
    **compare_visually_dataframes_kwargs,
)
# # Remove the sign and NaNs.
# diff_df = diff_df.replace([np.inf, -np.inf], np.nan).abs()
# # Check that data is the same.
# #diff_df.max().max()
display(hpandas.heatmap_df(diff_df.round(2)))

# %%
display(prod_target_position_df["holdings_shares"].dropna().head(10))

display(research_portfolio_df["holdings_shares"].dropna().head(10))

# %% [markdown]
# # Compare prices

# %%
if False:
    # Select a specific node and timestamp to analyze.
    #log_level = logging.INFO
    log_level = logging.DEBUG
    dag_node_names = oms.get_dag_node_names(dag_path_dict["prod"], log_level=log_level)
    #dag_node_name = dag_node_names[-1]
    dag_node_name = "predict.0.read_data"
    print(hprint.to_str("dag_node_name"))

    dag_node_timestamps = oms.get_dag_node_timestamps(
        dag_path_dict["prod"], dag_node_name, as_timestamp=True, log_level=log_level
    )

    dag_node_timestamp = dag_node_timestamps[-1]
    print("dag_node_timestamp=%s" % dag_node_timestamp)


    # Load DAG output for different experiments.
    dag_df_dict = oms.load_dag_outputs(dag_path_dict, dag_node_name, dag_node_timestamp, 
                                       start_timestamp, end_timestamp,
                                       log_level=logging.INFO)


    asset_id = 1030828978
    #asset_id = 1464553467
    dag_df_dict["prod"]["close"][asset_id]

    # 2022-10-28 06:11:00-04:00    0.4798
    # 2022-10-28 06:12:00-04:00    0.4802
    # 2022-10-28 06:13:00-04:00    0.4807
    # 2022-10-28 06:14:00-04:00    0.4809
    # 2022-10-28 06:15:00-04:00    0.4808

    print(dag_df_dict["prod"]["close"][asset_id]["2022-10-28 06:12:00-04:00": '2022-10-28 06:16:00-04:00'].mean())
    print(dag_df_dict["prod"]["close"][asset_id]["2022-10-28 06:11:00-04:00": '2022-10-28 06:15:00-04:00'].mean())

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

    # Load the data for the reconciliation date.
    # `ImClient` operates in UTC timezone.
    start_ts = pd.Timestamp("2022-10-28 06:10:00", tz="America/New_York")
    end_ts = start_ts + pd.Timedelta(days=1)
    columns = None
    filter_data_mode = "assert"
    full_symbols = ["binance::GMT_USDT"]
    df = im_client.read_data(
        full_symbols, start_ts, end_ts, columns, filter_data_mode
    )
    hpandas.df_to_str(df.head(10), num_rows=None, log_level=logging.INFO)

# %% [markdown]
# # Compare pairwise portfolio correlations

# %%
research_portfolio_df["holdings_shares"].head(10)

# %%
dtfmod.compute_correlations(
    research_portfolio_df.pct_change(),
    portfolio_dfs["prod"].pct_change(),
    allow_unequal_indices=True,
    allow_unequal_columns=True,
)

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
# Analyze per stock

# TODO(gp): Chack amp Master looking for get_asset_slice 

def plot_together(df1, df2, suffix):
    df1 = pd.DataFrame(df1)
    df2 = pd.DataFrame(df2)
    df = df1.merge(df2,
                   left_index=True, right_index=True, 
                   how="outer", suffixes=suffix)
    return df
    

asset_id = 1966583502
#display(oms.get_asset_slice(research_portfolio_df, asset_id).head(5))

#display(oms.get_asset_slice(portfolio_dfs["prod"], asset_id).head(5))

def add_prices(df):
    df = pd.DataFrame(df)
    df["computed_price"] = df["holdings_notional"] / df["holdings_shares"]
    return df


#research_portfolio_df = add_prices(research_portfolio_df)
#portfolio_dfs["prod"] = add_prices(portfolio_dfs["prod"])

#column = "executed_trades_notional"
column = "holdings_notional"
#column = "computed_price"
df1 = oms.get_asset_slice(research_portfolio_df, asset_id)
df1 = add_prices(df1)

df2 = oms.get_asset_slice(portfolio_dfs["prod"], asset_id)
df2 = add_prices(df2)

df = plot_together(df1, df2, ["_res", "_prod"])
#display(df.head(3))
#df = df[["price", "computed_price_res", "computed_price_prod"]].pct_change()
#df = df[["price", "computed_price_res", "computed_price_prod"]]
#df = df[["pnl_res", "pnl_prod"]]
df = df[["holdings_notional_res", "holdings_notional_prod"]]
display(df)
#df.plot()

#df.plot()


# %%
dtfmod.compute_correlations(
    research_portfolio_df,
    portfolio_dfs["sim"],
    allow_unequal_indices=True,
    allow_unequal_columns=True,
)

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

# %% [markdown]
# ## Target vs executed

# %%
#df1 = prod_target_position_df["target_holdings_shares"][asset_id].shift(1)
#df2 = prod_target_position_df["holdings_shares"][asset_id]

df1 = prod_target_position_df["target_holdings_shares"].shift(1)
df2 = prod_target_position_df["holdings_shares"]

#df1
df1 - df2

# %%
display(prod_target_position_df["holdings_shares"].loc["2022-11-01 09:10:00-04:00"])
display(prod_target_position_df["target_holdings_shares"].loc["2022-11-01 09:10:00-04:00"])

# %%
# We are getting the fills that correspond to the orders and to the change of holdings.
prod_target_position_df["holdings_shares"][asset_id].diff()

# %% [markdown]
# # Fill stats

# %%
fills = oms.compute_fill_stats(prod_target_position_df)
fills["underfill_share_count"].plot()

# %%
fills["underfill_share_count"].round(4)

# %%
print(fills.columns.levels[0])
fills["tracking_error_shares"]

# TODO(gp): Add a check to ensure that this is never positive.

# %%
fills["fill_rate"].head()

# %%
fills["fill_rate"].plot()

# %% [markdown]
# # Slippage

# %%
slippage = oms.compute_share_prices_and_slippage(portfolio_dfs["prod"])
slippage["slippage_in_bps"].plot()

# %%
slippage["slippage_in_bps"].stack().hist(bins=31)

# %% [markdown]
# # TCA

# %%
if config["meta"]["run_tca"]:
    tca = cofinanc.load_and_normalize_tca_csv(tca_csv)
    tca = cofinanc.compute_tca_price_annotations(tca, True)
    tca = cofinanc.pivot_and_accumulate_holdings(tca, "")
