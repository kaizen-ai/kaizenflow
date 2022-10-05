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
import pprint
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


# %%
def get_reconciliation_config(date_str: str, asset_class: str) -> cconfig.Config:
    """
    Get a reconciliation that is specific of an asset class.
    
    :param date_str: reconciliation date as str, e.g., `20221003`
    :param asset_class: either `equities` or `crypto`
    """
    # Set values for variables that are specific of an asset class.
    if asset_class == "crypto":
        # For crypto the TCA part is not implemented yet.
        run_tca = False
        #
        bar_duration = "5T"
        #
        root_dir = "/shared_data/prod_reconciliation"
        # TODO(Grisha): probably we should rename to `system_log_dir`.
        prod_dir = os.path.join(root_dir, date_str, "prod", "system_log_dir_scheduled__2022-10-03T10:00:00+00:00_2hours")
        data_dict = {
           "prod_dir": prod_dir,
           # For crypto we do not have a `candidate` so we just re-use prod.
           "cand_dir": prod_dir,
           "sim_dir": os.path.join(root_dir, date_str, "simulation", "system_log_dir"),
        }
        #
        fep_init_dict = {
            "price_col": "vwap",
            "prediction_col": "vwap.ret_0.vol_adj_2_hat",
            "volatility_col": "vwap.ret_0.vol",
        }
        quantization = "no_quantization"
        gmv = 700.0
        liquidate_at_end_of_day = False
    elif asset_class == "equities":
        run_tca = True
        #
        bar_duration = "15T"
        #
        root_dir = ""
        search_str = ""
        prod_dir_cmd = f"find {root_dir}/{date_str}/prod -name '{search_str}'"
        _, prod_dir = hsystem.system_to_string(prod_dir_cmd)
        cand_cmd = f"find {root_dir}/{date_str}/job.candidate.* -name '{search_str}'"
        _, cand_dir = hsystem.system_to_string(cand_cmd)
        data_dict = {
           "prod_dir": prod_dir,
           "cand_dir": cand_dir,
           "sim_dir": os.path.join(root_dir, date_str, "system_log_dir"),
        }
        #
        fep_init_dict = {
            "price_col": "twap",
            "prediction_col": "prediction",
            "volatility_col": "garman_klass_vol",
        }
        quantization = "nearest_share"
        gmv = 20000.0
        liquidate_at_end_of_day = True
    else:
        raise ValueError(f"Unsupported asset class={asset_class}")
    # Get a config.
    config_dict = {
        "meta": {
            "date_str": date_str,
            "asset_class": asset_class,
            "run_tca": run_tca,
            "bar_duration": bar_duration,
        },
        "load_data_config": data_dict,
        "research_forecast_evaluator_from_prices": {
            "init": fep_init_dict,
            "annotate_forecasts_kwargs": {
                "quantization": quantization,
                "burn_in_bars": 3,
                "style": "cross_sectional",
                "bulk_frac_to_remove": 0.0,
                "target_gmv": gmv,
                "liquidate_at_end_of_day": liquidate_at_end_of_day
            }
        }
    }
    
    config = cconfig.Config.from_dict(config_dict)
    return config


# %%
date_str = "20221004"
#asset_class = "equities"
asset_class = "crypto"    
    
config = get_reconciliation_config(date_str, asset_class)
print(config)

# %% [markdown]
# # Specify data to load

# %% run_control={"marked": true}
# TODO(Grisha): factor out common code.
prod_dir = config["load_data_config"]["prod_dir"]
print(prod_dir)
hdbg.dassert(prod_dir)
hdbg.dassert_dir_exists(prod_dir)

cand_dir = config["load_data_config"]["cand_dir"]
print(cand_dir)
hdbg.dassert(cand_dir)
hdbg.dassert_dir_exists(cand_dir)

# INV: The research flow can be computed from sim or cand or prod.
sim_dir = config["load_data_config"]["sim_dir"]
print(sim_dir)
hdbg.dassert(sim_dir)
hdbg.dassert_dir_exists(sim_dir)

# %%
# TODO(Grisha): factor out common code.

prod_portfolio_dir = os.path.join(prod_dir, "process_forecasts/portfolio")
hdbg.dassert_dir_exists(prod_portfolio_dir)
print("prod_portfolio_dir=", prod_portfolio_dir)
prod_dag_dir = os.path.join(prod_dir, "dag/node_io/node_io.data")
hdbg.dassert_dir_exists(prod_dag_dir)
#
cand_portfolio_dir = os.path.join(cand_dir, "process_forecasts/portfolio")
hdbg.dassert_dir_exists(cand_portfolio_dir)
print("cand_portfolio_dir=", cand_portfolio_dir)
cand_dag_dir = os.path.join(cand_dir, "dag/node_io/node_io.data")
hdbg.dassert_dir_exists(cand_dag_dir)
#
sim_portfolio_dir = os.path.join(sim_dir, "process_forecasts/portfolio")
hdbg.dassert_dir_exists(sim_portfolio_dir)
print("sim_portfolio_dir=", sim_portfolio_dir)
sim_dag_dir = os.path.join(sim_dir, "dag/node_io/node_io.data")
hdbg.dassert_dir_exists(sim_dag_dir)

# TODO(gp): Load the TCA data for crypto.
if config["meta"]["run_tca"]:
    tca_csv = os.path.join(root_dir, date_str, "tca/sau1_tca.csv")
    hdbg.dassert_file_exists(tca_csv)

# %%
portfolio_path_dict = {
    "prod": prod_portfolio_dir,
    "cand": cand_portfolio_dir,
    "sim": sim_portfolio_dir,
}

# %%
# TODO(gp): @Grisha infer this from the data from prod Portfolio df.

start_timestamp = pd.Timestamp(date_str + " 10:05:00", tz="America/New_York")
_LOG.info("start_timestamp=%s", start_timestamp)
end_timestamp = pd.Timestamp(date_str + " 12:00:00", tz="America/New_York")
_LOG.info("end_timestamp=%s", end_timestamp)


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
# GOAL: We should be able to specify what exactly we want to run (e.g., prod, cand, sim)
# because not everything is always available or important (e.g., for cc we don't have candidate,
# for equities we don't always have sim).
# INV: prod_dag_df -> dag_df["prod"], cand_dag_df -> dag_df["cand"]

# %%
prod_dag_df = get_latest_output_from_last_dag_node(prod_dag_dir)
hpandas.df_to_str(prod_dag_df, num_rows=5, log_level=logging.INFO)


# %%
sim_dag_df = get_latest_output_from_last_dag_node(sim_dag_dir)
hpandas.df_to_str(sim_dag_df, num_rows=5, log_level=logging.INFO)

# %%
prod_sim_dag_corr = dtfmod.compute_correlations(
    prod_dag_df,
    sim_dag_df,
)
hpandas.df_to_str(
    prod_sim_dag_corr.min(),
    num_rows=None,
    precision=3,
    log_level=logging.INFO,
)

# %%
# Make sure they are exactly the same.
(prod_dag_df - sim_dag_df).abs().max().max()

# %% run_control={"marked": false}
# TODOO(gp): @grisha
# Problem: given two multi-index dfs, we want to compare how similar they are

# Check if they have the same columns in the same order
#  - switch to ignore certain columns, or select the intersection 
#  - switch to reorder the columns to sort them

# Check if they have the same index
#  - switch to perform intersection
#  - if there is a mismatch it should be one is included in the other

#  - are they any missing value based on the frequency

# Check if they are exactly the same, e.g., the difference is less than a threshold <1e-6.
#   Show the rows with the max difference (use the `differ_visually_...`)
#   Allow to subset by columns (e.g., close)

# %%
# TODO(gp): Automate the burn-in correlation

# TODO(gp): Handle the outliers

# %%
# TODO(gp): @grisha

# Given two multi-index dfs, allow to slice the values by index or by column
# Create a df with sliced 2 columns or rows and do the diff so that it's easy to plot / inspect
#
# #col_name = "price"
# col_name = "executed_trades_notional"
# #asset_id = 1030828978
# #asset_id = 5115052901
# asset_id = 5118394986
# #df1 = adapted_sim_df[col_name][asset_id]
# df1 = adapted_prod_df[col_name][asset_id]
# df2 = research_portfolio_df[col_name][asset_id]

# (df1 - df2).dropna().plot()

# df = pd.DataFrame(df1).merge(pd.DataFrame(df2), how="outer", left_index=True, right_index=True, suffixes=["_prod", "_research"])

# #df["diff"] = df["1030828978_prod"] - df["1030828978_research"]

# #display(df)

# df.dropna().plot()

# %%
# TODO(gp): Add function to compare duration of different dfs
# E.g., duration_df = compute_duration_df(tag_to_df)
#  Compute min / max index
#  Compute min / max index with all values non-nans
#. Missing row
#  The output is multi-index indexed by tag and has (min_idx, max_idx, min_valid_idx, max_valid_idx)
#duration_df = pd.MultiIndex

# %% [markdown]
# # Compute research portfolio equivalent

# %%
# TODO(gp): to_str?
print('config["research_forecast_evaluator_from_prices"]["init"]=\n' +
      hprint.indent(
          str(config["research_forecast_evaluator_from_prices"]["init"])))
fep = dtfmod.ForecastEvaluatorFromPrices(
    **config["research_forecast_evaluator_from_prices"]["init"])

# %%
# TODO(gp): to_str?
print(hprint.to_str('config["research_forecast_evaluator_from_prices"]["annotate_forecasts_kwargs"]',
                    char_separator="\n"))

research_portfolio_df, research_portfolio_stats_df = fep.annotate_forecasts(
    prod_dag_df,
    **config["research_forecast_evaluator_from_prices"]["annotate_forecasts_kwargs"],
    compute_extended_stats=True
)

# %%
# TODO(gp): We should have a function for this
research_portfolio_df.transpose().xs(1030828978, level=1).transpose()

# %%
# TODO(gp): Consider adding printing the levels to df_to_str.
# TODO(gp): Consider showing only the first few columns of second level (e.g., num_cols=2 to see 2
#. assets)
print(research_portfolio_df.columns.levels[0])

# TODO(gp): num_rows=2 by default
# TODO(gp): Add switch skip_nans
hpandas.df_to_str(research_portfolio_df, num_rows=2, log_level=logging.INFO)

# price, volatility, prediction are the inputs from the DAG (that are propagated)
# the rest is computed using the parameters for the ForecastEvaluator

# %%
hpandas.df_to_str(research_portfolio_stats_df, log_level=logging.INFO)

# %%
# TODO(gp): Move the sorting to annotate_forecasts only for the second level.
# Ideally, we should have assume that things are sorted and if they are not asserts.
# Then there is a switch to acknowledge this problem and solve it.
research_portfolio_df = research_portfolio_df.sort_index(axis=1, level=1)

# %% [markdown]
# # Orders

# %%
prod_order_df = oms.ForecastProcessor.read_logged_orders(
    portfolio_path_dict["prod"] + "/.."
)
hpandas.df_to_str(prod_order_df, log_level=logging.INFO)

# %%
sim_order_df = oms.ForecastProcessor.read_logged_orders(
    portfolio_path_dict["sim"] + "/.."
)
hpandas.df_to_str(sim_order_df, log_level=logging.INFO)

# %%
research_portfolio_df["executed_trades_shares"]

# %%
prod_order_df = prod_order_df.pivot(
    index="end_timestamp",
    columns="asset_id",
    values="diff_num_shares",
)
freq = "5T"
prod_order_df.index = prod_order_df.index.round(freq)

sim_order_df = sim_order_df.pivot(
    index="end_timestamp",
    columns="asset_id",
    values="diff_num_shares",
)
sim_order_df.index = sim_order_df.index.round(freq)

# %%
asset_id = 1030828978

# %% [markdown]
# # Load logged portfolios

# %%
portfolio_config_dict = {
    "start_timestamp": start_timestamp,
    "end_timestamp": end_timestamp,
    "freq": config["meta"]["bar_duration"],
    "normalize_bar_times": True,
}
portfolio_config_dict

# %%
# Load the 4 portfolios.
portfolio_dfs = {}
portfolio_stats_dfs = {}
for name, path in portfolio_path_dict.items():
    _LOG.info("Processing portfolio=%s path=%s", name, path)
    portfolio_df, portfolio_stats_df = oms.load_portfolio_artifacts(
        path,
        **portfolio_config_dict,
    )
    # TODO(gp): We need to understand why this is not ordered.
    portfolio_df = portfolio_df.sort_index(axis=1)
    
    portfolio_dfs[name] = portfolio_df
    portfolio_stats_dfs[name] = portfolio_stats_df
    
portfolio_df = research_portfolio_df.loc[start_timestamp:end_timestamp]
portfolio_df = portfolio_df.sort_index(axis=1)
portfolio_dfs["research"] = portfolio_df
portfolio_stats_dfs["research"] = research_portfolio_stats_df.loc[start_timestamp:end_timestamp]

# Multi-index of the stats.
portfolio_stats_df = pd.concat(portfolio_stats_dfs, axis=1)

# %%
print(portfolio_dfs.keys())

# %%
hpandas.df_to_str(portfolio_stats_df, num_rows=2, log_level=logging.INFO)

# %%
# TODO(gp): For some reason the prod index is not ordered.
hpandas.df_to_str(portfolio_dfs["prod"]["holdings", 1030828978], num_rows=4, log_level=logging.INFO)
hpandas.df_to_str(portfolio_dfs["research"]["holdings_shares", 1030828978], num_rows=4, log_level=logging.INFO)

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
adapted_prod_df = oms.adapt_portfolio_object_df_to_forecast_evaluator_df(portfolio_dfs["prod"])
adapted_cand_df = oms.adapt_portfolio_object_df_to_forecast_evaluator_df(portfolio_dfs["cand"])
adapted_sim_df = oms.adapt_portfolio_object_df_to_forecast_evaluator_df(portfolio_dfs["sim"])

# %%
adapted_prod_df.columns.levels

# %%
adapted_prod_df.columns.levels[0]

# %%
adapted_prod_df = adapted_prod_df.sort_index(axis=1)
research_portfolio_df = research_portfolio_df.sort_index(axis=1)

# %%
col_name = "holdings_shares"
#col_name = "price"
display(adapted_prod_df[col_name].tail(3))
display(research_portfolio_df[col_name].tail(3))

# %%
#col_name = "pnl"
#col_name = "executed_trades_shares"
col_name = "holdings_shares"
dtfmod.compute_correlations(
    adapted_prod_df.iloc[1:],
    research_portfolio_df.iloc[1:],
    allow_unequal_indices=True,
    allow_unequal_columns=True,
).sort_values([col_name], ascending=False)

# %%
#col_name = "price"
col_name = "executed_trades_notional"
#asset_id = 1030828978
#asset_id = 5115052901
asset_id = 5118394986
#df1 = adapted_sim_df[col_name][asset_id]
df1 = adapted_prod_df[col_name][asset_id]
df2 = research_portfolio_df[col_name][asset_id]

(df1 - df2).dropna().plot()

df = pd.DataFrame(df1).merge(pd.DataFrame(df2), how="outer", left_index=True, right_index=True, suffixes=["_prod", "_research"])

#df["diff"] = df["1030828978_prod"] - df["1030828978_research"]

#display(df)

df.dropna().plot()

# %%
dtfmod.compute_correlations(
    #research_portfolio_df,
    adapted_sim_df.iloc[1:],
    research_portfolio_df.iloc[1:],
    allow_unequal_indices=True,
    allow_unequal_columns=True,
).sort_values(["pnl"], ascending=False)

# %%
dtfmod.compute_correlations(
    adapted_prod_df,
    adapted_sim_df,
    allow_unequal_indices=False,
    allow_unequal_columns=False,
).sort_values(["pnl"], ascending=False)

# %%
dtfmod.compute_correlations(
    research_portfolio_df,
    adapted_sim_df,
    allow_unequal_indices=True,
    allow_unequal_columns=True,
).sort_values(["pnl"], ascending=False)

# %%
if config["meta"]["run_tca"]:
    tca = cofinanc.load_and_normalize_tca_csv(tca_csv)
    tca = cofinanc.compute_tca_price_annotations(tca, True)
    tca = cofinanc.pivot_and_accumulate_holdings(tca, "")
