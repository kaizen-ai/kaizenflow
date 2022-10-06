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

import pandas as pd

import core.config as cconfig
import core.finance as cofinanc
import core.plotting as coplotti
import dataflow.model as dtfmod
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hpickle as hpickle
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
# TODO(Grisha): factor out common code.
prod_dir = config["load_data_config"]["prod_dir"]
_LOG.info("Prod_dir=%s", prod_dir)
hdbg.dassert(prod_dir)
hdbg.dassert_dir_exists(prod_dir)

cand_dir = config["load_data_config"]["cand_dir"]
_LOG.info("Cand_dir=%s", cand_dir)
hdbg.dassert(cand_dir)
hdbg.dassert_dir_exists(cand_dir)

sim_dir = config["load_data_config"]["sim_dir"]
_LOG.info("Sim_dir=%s", sim_dir)
hdbg.dassert(sim_dir)
hdbg.dassert_dir_exists(sim_dir)

# %%
# TODO(Grisha): factor common code.
# TODO(Grisha): diff configs.
config_name = "system_config.input.values_as_strings.pkl"

prod_config_path = os.path.join(prod_dir, config_name)
prod_config_pkl = hpickle.from_pickle(prod_config_path)
prod_config = cconfig.Config.from_dict(prod_config_pkl)
#
sim_config_path = os.path.join(sim_dir, config_name)
sim_config_pkl = hpickle.from_pickle(sim_config_path)
sim_config = cconfig.Config.from_dict(sim_config_pkl)

# %%
# TODO(Grisha): factor out common code.
prod_portfolio_dir = os.path.join(prod_dir, "process_forecasts/portfolio")
hdbg.dassert_dir_exists(prod_portfolio_dir)
_LOG.info("prod_portfolio_dir=%s", prod_portfolio_dir)
prod_dag_dir = os.path.join(prod_dir, "dag/node_io/node_io.data")
hdbg.dassert_dir_exists(prod_dag_dir)
#
cand_portfolio_dir = os.path.join(cand_dir, "process_forecasts/portfolio")
hdbg.dassert_dir_exists(cand_portfolio_dir)
_LOG.info("cand_portfolio_dir=%s", cand_portfolio_dir)
cand_dag_dir = os.path.join(cand_dir, "dag/node_io/node_io.data")
hdbg.dassert_dir_exists(cand_dag_dir)
#
sim_portfolio_dir = os.path.join(sim_dir, "process_forecasts/portfolio")
hdbg.dassert_dir_exists(sim_portfolio_dir)
_LOG.info("sim_portfolio_dir=%s", sim_portfolio_dir)
sim_dag_dir = os.path.join(sim_dir, "dag/node_io/node_io.data")
hdbg.dassert_dir_exists(sim_dag_dir)

# TODO(gp): Load the TCA data for crypto.
if config["meta"]["run_tca"]:
    tca_csv = os.path.join(root_dir, date_str, "tca/sau1_tca.csv")
    hdbg.dassert_file_exists(tca_csv)

# %%
# This dict points to `system_log_dir/process_forecasts/portfolio` for different experiments.
portfolio_path_dict = {
    "prod": prod_portfolio_dir,
    "cand": cand_portfolio_dir,
    "sim": sim_portfolio_dir,
}

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
# TODO(gp): @grisha move to oms/reconciliation.py


def get_latest_output_from_last_dag_node(dag_dir: str) -> pd.DataFrame:
    """
    Retrieve the most recent output from the last DAG node.

    This function relies on our file naming conventions.
    """
    parquet_files = list(
        filter(lambda x: "parquet" in x, sorted(os.listdir(cand_dag_dir)))
    )
    _LOG.info("Tail of files found=%s", parquet_files[-3:])
    file_name = parquet_files[-1]
    _LOG.info("DAG file selected=%s", file_name)
    dag_parquet_path = os.path.join(cand_dag_dir, file_name)
    # _LOG.info("DAG parquet path=%s", dag_parquet_path)
    dag_df = pd.read_parquet(dag_parquet_path)
    return dag_df


# %%
# TODO(gp): @Grisha
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

# %%
hpandas.df_to_str(
    prod_sim_dag_corr.min(),
    num_rows=None,
    precision=3,
    log_level=logging.INFO,
)

# %%
# Make sure they are exactly the same.
(prod_dag_df - sim_dag_df).abs().max().max()

# %% [markdown]
# # Compute research portfolio equivalent

# %%
fep = dtfmod.ForecastEvaluatorFromPrices(
    **config["research_forecast_evaluator_from_prices"]["init"]
)

# %%
research_portfolio_df, research_portfolio_stats_df = fep.annotate_forecasts(
    prod_dag_df,
    **config["research_forecast_evaluator_from_prices"][
        "annotate_forecasts_kwargs"
    ],
    compute_extended_stats=True,
)
# TODO(gp): Move it to annotate_forecasts?
research_portfolio_df = research_portfolio_df.sort_index(axis=1)
#
hpandas.df_to_str(research_portfolio_stats_df, num_rows=5, log_level=logging.INFO)

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
# TODO(gp): @grisha move to library.

# Load the 4 portfolios.
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
portfolio_dfs["research"] = research_portfolio_df.loc[
    start_timestamp:end_timestamp
]
portfolio_stats_dfs["research"] = research_portfolio_stats_df.loc[
    start_timestamp:end_timestamp
]
portfolio_stats_df = pd.concat(portfolio_stats_dfs, axis=1)
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
adapted_prod_df = oms.adapt_portfolio_object_df_to_forecast_evaluator_df(
    portfolio_dfs["prod"]
)
adapted_cand_df = oms.adapt_portfolio_object_df_to_forecast_evaluator_df(
    portfolio_dfs["cand"]
)
adapted_sim_df = oms.adapt_portfolio_object_df_to_forecast_evaluator_df(
    portfolio_dfs["sim"]
)

# %%
dtfmod.compute_correlations(
    research_portfolio_df,
    adapted_prod_df,
    allow_unequal_indices=True,
    allow_unequal_columns=True,
)

# %%
dtfmod.compute_correlations(
    adapted_prod_df,
    adapted_sim_df,
    allow_unequal_indices=False,
    allow_unequal_columns=False,
)

# %%
dtfmod.compute_correlations(
    research_portfolio_df,
    adapted_sim_df,
    allow_unequal_indices=True,
    allow_unequal_columns=True,
)

# %%
if config["meta"]["run_tca"]:
    tca = cofinanc.load_and_normalize_tca_csv(tca_csv)
    tca = cofinanc.compute_tca_price_annotations(tca, True)
    tca = cofinanc.pivot_and_accumulate_holdings(tca, "")

# %%
