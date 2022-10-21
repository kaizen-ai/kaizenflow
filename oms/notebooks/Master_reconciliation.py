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
from typing import Dict

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
# The dict points to `system_log_dir` for different experiments.
system_log_path_dict = dict(config["system_log_path"].to_dict())
system_log_path_dict

# %%
# TODO(Grisha): factor common code.
# TODO(Grisha): diff configs.
config_name = "system_config.input.values_as_strings.pkl"

prod_config_path = os.path.join(system_log_path_dict["prod"], config_name)
prod_config_pkl = hpickle.from_pickle(prod_config_path)
prod_config = cconfig.Config.from_dict(prod_config_pkl)
#
sim_config_path = os.path.join(system_log_path_dict["sim"], config_name)
sim_config_pkl = hpickle.from_pickle(sim_config_path)
sim_config = cconfig.Config.from_dict(sim_config_pkl)

# %%
# TODO(gp): @grisha move to `oms/reconciliation.py`.


def get_system_log_paths(
    system_log_path_dict: Dict[str, str], data_type: str
) -> Dict[str, str]:
    """
    Get paths to data inside a system log dir.

    :param system_log_path_dict: system log dirs paths for different experiments, e.g.,
        `{"prod": "/shared_data/system_log_dir", "sim": ...}`
    :param data_type: either "dag" to load DAG output or "portfolio" to load Portfolio
    :return: dir paths inside system log dir for different experiments, e.g.,
        `{"prod": "/shared_data/system_log_dir/process_forecasts/portfolio", "sim": ...}`
    """
    data_path_dict = {}
    if data_type == "portfolio":
        dir_name = "process_forecasts/portfolio"
    elif data_type == "dag":
        dir_name = "dag/node_io/node_io.data"
    else:
        raise ValueError(f"Unsupported data type={data_type}")
    for k, v in system_log_path_dict.items():
        cur_dir = os.path.join(v, dir_name)
        hdbg.dassert_dir_exists(cur_dir)
        data_path_dict[k] = cur_dir
    return data_path_dict


# %%
# This dict points to `system_log_dir/process_forecasts/portfolio` for different experiments.
data_type = "portfolio"
portfolio_path_dict = get_system_log_paths(system_log_path_dict, data_type)
portfolio_path_dict

# %%
# This dict points to `system_log_dir/dag/node_io/node_io.data` for different experiments.
data_type = "dag"
dag_path_dict = get_system_log_paths(system_log_path_dict, data_type)
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
end_timestamp = pd.Timestamp(date_str + " 07:50:00", tz="America/New_York")
_LOG.info("end_timestamp=%s", end_timestamp)


# %% [markdown]
# # Compare DAG io

# %%
# TODO(gp): @grisha move to `oms/reconciliation.py`.


def get_latest_output_from_last_dag_node(dag_dir: str) -> pd.DataFrame:
    """
    Retrieve the most recent output from the last DAG node.

    This function relies on our file naming conventions.
    """
    hdbg.dassert_dir_exists(dag_dir)
    parquet_files = list(
        filter(lambda x: "parquet" in x, sorted(os.listdir(dag_dir)))
    )
    _LOG.info("Tail of files found=%s", parquet_files[-3:])
    file_name = parquet_files[-1]
    dag_parquet_path = os.path.join(dag_dir, file_name)
    _LOG.info("DAG parquet path=%s", dag_parquet_path)
    dag_df = pd.read_parquet(dag_parquet_path)
    return dag_df


# %%
# Load DAG output for different experiments.
dag_df_dict = {}
for name, path in dag_path_dict.items():
    dag_df_dict[name] = get_latest_output_from_last_dag_node(path)
hpandas.df_to_str(dag_df_dict["prod"], num_rows=5, log_level=logging.INFO)

# %%
dag_df_dict["prod"].index.max()

# %%
dag_df_dict["sim"].index.max()

# %%
dag_df_dict["prod"] = dag_df_dict["prod"].loc[start_timestamp: end_timestamp]
dag_df_dict["sim"] = dag_df_dict["sim"].loc[start_timestamp: end_timestamp]

# %%
prod_sim_dag_corr = dtfmod.compute_correlations(
    dag_df_dict["prod"],
    dag_df_dict["sim"],
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
(dag_df_dict["prod"] - dag_df_dict["sim"]).abs().max().max()

# %% [markdown]
# # Compute research portfolio equivalent

# %%
fep = dtfmod.ForecastEvaluatorFromPrices(
    **config["research_forecast_evaluator_from_prices"]["init"]
)

# %%
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
import oms
prod_target_position_df = oms.load_target_positions(
    prod_forecast_dir,
    start_timestamp,
    end_timestamp,
    bar_duration,
    normalize_bar_times=True
)

# %%
research_portfolio_df.columns.levels[0]

# %%
research_portfolio_df.sort_index(axis=1)["holdings_shares"].head(5)

# %%
portfolio_dfs["sim"].sort_index(axis=1)["holdings_shares"][6051632686].head(5)

# %%
portfolio_dfs["prod"].sort_index(axis=1)["holdings_shares"][6051632686].head(5)

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
