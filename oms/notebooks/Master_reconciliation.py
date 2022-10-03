# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.8
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
from typing import List, Tuple

import pandas as pd

import core.config as cconfig
import core.finance as cofinanc
import core.plotting as coplotti
import dataflow.model as dtfmod
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hsystem as hsystem
import oms as oms

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Specify data to load

# %% run_control={"marked": true}
#date_str = "20220928"
date_str = "20221003"
root_dir = ""
search_str = ""

#cmd = f"find {root_dir}/{date_str}/prod -name '{search_str}'"
#rc, prod_dir = hsystem.system_to_string(cmd)
prod_dir = "/data/shared/prod_reconciliation/20221003/prod"
prod_dir = prod_dir.replace("/data/shared/", "/shared_data/")
hdbg.dassert(prod_dir)
hdbg.dassert_dir_exists(prod_dir)

#cmd = f"find {root_dir}/{date_str}/job.candidate.* -name '{search_str}'"
#rc, cand_dir = hsystem.system_to_string(cmd)
cand_dir = "/data/shared/prod_reconciliation/20221003/prod"
cand_dir = cand_dir.replace("/data/shared/", "/shared_data/")
hdbg.dassert(cand_dir)
hdbg.dassert_dir_exists(cand_dir)

#sim_dir = os.path.join(f"{root_dir}/{date_str}/system_log_dir")
sim_dir = "/data/shared/prod_reconciliation/20221003/simulation/system_log_dir"
sim_dir = sim_dir.replace("/data/shared/", "/shared_data/")
hdbg.dassert(sim_dir)
hdbg.dassert_dir_exists(sim_dir)

# %%
prod_portfolio_dir = os.path.join(prod_dir, "process_forecasts/portfolio")
hdbg.dassert_dir_exists(prod_portfolio_dir)
print(prod_portfolio_dir)
prod_dag_dir = os.path.join(prod_dir, "dag/node_io/node_io.data")
hdbg.dassert_dir_exists(prod_dag_dir)
#
cand_portfolio_dir = os.path.join(cand_dir, "process_forecasts/portfolio")
hdbg.dassert_dir_exists(cand_portfolio_dir)
print(cand_portfolio_dir)
cand_dag_dir = os.path.join(cand_dir, "dag/node_io/node_io.data")
hdbg.dassert_dir_exists(cand_dag_dir)
#
sim_portfolio_dir = os.path.join(sim_dir, "process_forecasts/portfolio")
hdbg.dassert_dir_exists(sim_portfolio_dir)
print(sim_portfolio_dir)
sim_dag_dir = os.path.join(sim_dir, "dag/node_io/node_io.data")
hdbg.dassert_dir_exists(sim_dag_dir)
#
# tca_csv = os.path.join(root_dir, date_str, "tca/sau1_tca.csv")
# hdbg.dassert_file_exists(tca_csv)

# %%
portfolio_path_dict = {
    "prod": prod_portfolio_dir,
    "cand": cand_portfolio_dir,
    "sim": sim_portfolio_dir,
}

# %%
start_timestamp = pd.Timestamp(date_str + " 06:05:00", tz="America/New_York")
_LOG.info("start_timestamp=%s", start_timestamp)
end_timestamp = pd.Timestamp(date_str + " 08:00:00", tz="America/New_York")
_LOG.info("end_timestamp=%s", end_timestamp)

# %%
#bar_duration = "15T"
bar_duration = "5T"


# %% [markdown]
# # Compare DAG io

# %%
def get_latest_output_from_last_dag_node(dag_dir: str) -> pd.DataFrame:
    """
    Retrieve the most recent output from the last DAG node.

    This function relies on our file naming conventions.
    """
    parquet_files = list(filter(lambda x: "parquet" in x, sorted(os.listdir(cand_dag_dir))))
    _LOG.info("Tail of files found=%s", parquet_files[-3:])
    file_name = parquet_files[-1]
    _LOG.info("DAG file selected=%s", file_name)
    dag_parquet_path = os.path.join(cand_dag_dir, file_name)
    # _LOG.info("DAG parquet path=%s", dag_parquet_path)
    dag_df = pd.read_parquet(dag_parquet_path)
    return dag_df


# %%
dag_df.columns.levels[0]


# %%
dag_dir = prod_dag_dir
if False:
    stage = "0.read_data"
    target_cols = [
        "ask",
        "bid",
        "close",
        "day_num_spread",
        "day_spread",
        "high",
        "low",
        "notional",
        "open",
        "sided_ask_count",
        "sided_bid_count",
        "start_time",
        "volume",
    ]
# stage = "2.zscore"
stage = "8.process_forecasts"
# target_cols = [
#     "close",
#     "close_vwap",
#     "day_num_spread",
#     "day_spread",
#     "garman_klass_vol",
#     "high",
#     "low",
#     "notional",
#     "open",
#     "prediction",
#     "twap",
#     "volume",
# ]
target_cols = ['close', 'close.ret_0', 'twap', 'twap.ret_0', 'volume', 'vwap', 'vwap.ret_0', 'vwap.ret_0.vol', 'vwap.ret_0.vol_adj', 'vwap.ret_0.vol_adj.c', 'vwap.ret_0.vol_adj.c.lag0', 'vwap.ret_0.vol_adj.c.lag1', 'vwap.ret_0.vol_adj.c.lag2', 'vwap.ret_0.vol_adj.c.lag3', 'vwap.ret_0.vol_adj_2_hat']
timestamp = "20221003_080000"

file_name = f"predict.{stage}.df_out.{timestamp}.csv"
file_name = os.path.join(dag_dir, file_name)
print(file_name)
dag_df = pd.read_csv(file_name, parse_dates=True, index_col=0, header=[0, 1])

# dag_df = dag_df[start_timestamp:end_timestamp]

display(dag_df.head(3))

# print(dag_df.columns.levels[0])
# print(sim_dag_df.columns.levels[0])
# dag_df.drop(labels=["end_time"], axis=1, level=0, inplace=True, errors="raise")
asset_ids = dag_df.columns.levels[1].tolist()
# for col in dag_df.columns:
#     if col[0] in target_cols:
#     columns.append()
import itertools

columns = list(itertools.product(target_cols, asset_ids))
dag_df = dag_df[pd.MultiIndex.from_tuples(columns)].copy()
hpandas.df_to_str(dag_df, log_level=logging.INFO)
dag_df.to_csv("prod_tmp.csv")
dag_df = pd.read_csv("prod_tmp.csv", index_col=0, header=[0, 1])

asset_ids = map(int, asset_ids)
columns = list(itertools.product(target_cols, asset_ids))
columns = pd.MultiIndex.from_tuples(columns)
dag_df.columns = columns

dag_df.index = pd.to_datetime(dag_df.index)
dag_df.index = dag_df.index.tz_convert("America/New_York")

prod_dag_df = dag_df

# %%
dag_dir = sim_dag_dir
if False:
    stage = "0.read_data"
    target_cols = [
        "ask",
        "bid",
        "close",
        "day_num_spread",
        "day_spread",
        "high",
        "low",
        "notional",
        "open",
        "sided_ask_count",
        "sided_bid_count",
        "start_time",
        "volume",
    ]
# stage = "2.zscore"
stage = "8.process_forecasts"
# target_cols = [
#     "close",
#     "close_vwap",
#     "day_num_spread",
#     "day_spread",
#     "garman_klass_vol",
#     "high",
#     "low",
#     "notional",
#     "open",
#     "prediction",
#     "twap",
#     "volume",
# ]
target_cols = ['close', 'close.ret_0', 'twap', 'twap.ret_0', 'volume', 'vwap', 'vwap.ret_0', 'vwap.ret_0.vol', 'vwap.ret_0.vol_adj', 'vwap.ret_0.vol_adj.c', 'vwap.ret_0.vol_adj.c.lag0', 'vwap.ret_0.vol_adj.c.lag1', 'vwap.ret_0.vol_adj.c.lag2', 'vwap.ret_0.vol_adj.c.lag3', 'vwap.ret_0.vol_adj_2_hat']
timestamp = "20221003_080000"

file_name = f"predict.{stage}.df_out.{timestamp}.csv"
file_name = os.path.join(dag_dir, file_name)
print(file_name)
dag_df = pd.read_csv(file_name, parse_dates=True, index_col=0, header=[0, 1])

# dag_df = dag_df[start_timestamp:end_timestamp]

display(dag_df.head(3))

# print(dag_df.columns.levels[0])
# print(sim_dag_df.columns.levels[0])
# dag_df.drop(labels=["end_time"], axis=1, level=0, inplace=True, errors="raise")
asset_ids = dag_df.columns.levels[1].tolist()
# for col in dag_df.columns:
#     if col[0] in target_cols:
#     columns.append()
import itertools

columns = list(itertools.product(target_cols, asset_ids))
dag_df = dag_df[pd.MultiIndex.from_tuples(columns)].copy()
hpandas.df_to_str(dag_df, log_level=logging.INFO)
dag_df.to_csv("prod_tmp.csv")
dag_df = pd.read_csv("prod_tmp.csv", index_col=0, header=[0, 1])

asset_ids = map(int, asset_ids)
columns = list(itertools.product(target_cols, asset_ids))
columns = pd.MultiIndex.from_tuples(columns)
dag_df.columns = columns

dag_df.index = pd.to_datetime(dag_df.index)
dag_df.index = dag_df.index.tz_convert("America/New_York")

cand_dag_df = dag_df

# %%
#cand_dag_df = get_latest_output_from_last_dag_node(cand_dag_dir)
#hpandas.df_to_str(cand_dag_df, log_level=logging.INFO)

# %%
#prod_dag_df = get_latest_output_from_last_dag_node(prod_dag_dir)
#hpandas.df_to_str(prod_dag_df, log_level=logging.INFO)

# %%
prod_cand_dag_corr = dtfmod.compute_correlations(
    prod_dag_df,
    cand_dag_df,
)

# %%
prod_cand_dag_corr.min()

# %% [markdown]
# # Compute research portfolio equivalent

# %%
fep_dict = {
    "init": {
        #"price_col": "twap",
        #"prediction_col": "prediction",
        #"volatility_col": "garman_klass_vol",
        "price_col": "vwap",
        "prediction_col": "vwap.ret_0.vol_adj_2_hat",
        "volatility_col": "vwap.ret_0.vol",
    },
    "annotate_forecasts_kwargs": {
        #"quantization": "nearest_share",
        "quantization": "no_quantization",
        "burn_in_bars": 3,
        #
        "style": "cross_sectional",
        "bulk_frac_to_remove": 0.0,
        "target_gmv": 700.0,
    },
}

# %%
fep = dtfmod.ForecastEvaluatorFromPrices(**fep_dict["init"])

# %%
research_portfolio_df, research_portfolio_stats_df = fep.annotate_forecasts(
    prod_dag_df,
    **fep_dict["annotate_forecasts_kwargs"],
    compute_extended_stats=True
)

# %%
hpandas.df_to_str(research_portfolio_stats_df, log_level=logging.INFO)

# %% [markdown]
# # Load logged portfolios

# %%
portfolio_config_dict = {
    "start_timestamp": start_timestamp,
    "end_timestamp": end_timestamp,
    "freq": bar_duration,
    "normalize_bar_times": True,
}

# %%
portfolio_dfs = {}
portfolio_stats_dfs = {}
for name, path in portfolio_path_dict.items():
    _LOG.info("Processing portfolio=%s path=%s", name, path)
    portfolio_df, portfolio_stats_df = oms.load_portfolio_artifacts(
        path,
        **portfolio_config_dict,
    )
    portfolio_dfs[name] = portfolio_df
    portfolio_stats_dfs[name] = portfolio_stats_df
portfolio_dfs["research"] = research_portfolio_df.loc[start_timestamp:end_timestamp]
portfolio_stats_dfs["research"] = research_portfolio_stats_df.loc[start_timestamp:end_timestamp]
portfolio_stats_df = pd.concat(portfolio_stats_dfs, axis=1)

# %%
hpandas.df_to_str(portfolio_stats_df, log_level=logging.INFO)

# %%
bars_to_burn = 1
coplotti.plot_portfolio_stats(portfolio_stats_df.iloc[bars_to_burn:])

# %%
stats_computer = dtfmod.StatsComputer()
stats_sxs, _ = stats_computer.compute_portfolio_stats(
    portfolio_stats_df.iloc[bars_to_burn:], bar_duration
)
display(stats_sxs)

# %% [markdown]
# # Compare pairwise portfolio correlations

# %%
adapted_prod_df = oms.adapt_portfolio_object_df_to_forecast_evaluator_df(portfolio_dfs["prod"])
adapted_cand_df = oms.adapt_portfolio_object_df_to_forecast_evaluator_df(portfolio_dfs["cand"])
adapted_sim_df = oms.adapt_portfolio_object_df_to_forecast_evaluator_df(portfolio_dfs["sim"])

# %%
research_portfolio_df

# %%
dtfmod.compute_correlations(
    research_portfolio_df,
    adapted_prod_df,
    allow_unequal_indices=True,
    allow_unequal_columns=True,
)

# %%
portfolio_dfs["prod"].columns.levels[0]

# %%
dtfmod.compute_correlations(
    adapted_prod_df,
    adapted_cand_df,
    allow_unequal_indices=False,
    allow_unequal_columns=False,
)


# %%
def get_asset_id_slice(df: pd.DataFrame, asset_id: int) -> pd.DataFrame:
    return df.T.xs(asset_id, level=1).T


# %%
asset_id = 13684
get_asset_id_slice(adapted_prod_df, asset_id)

# %%
get_asset_id_slice(adapted_cand_df, asset_id)

# %%
# tca = cofinanc.load_and_normalize_tca_csv(tca_csv)
# tca = cofinanc.compute_tca_price_annotations(tca, True)
# tca = cofinanc.pivot_and_accumulate_holdings(tca, "")

# %%
