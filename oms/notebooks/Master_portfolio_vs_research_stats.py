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

# %% [markdown]
# Compare
# - ForecastEvaluator output (research pnl)
# - a Portfolio
#
# It can be used:
# - In the daily reconciliation flow to compare
#     - ForecastEvaluator coming from a simulation
#     - portfolio comes from a production system

# %% run_control={"marked": true}
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

import logging
import os

import pandas as pd

import core.config as cconfig
import core.plotting as coplotti
import dataflow.model as dtfmod
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import oms as oms

# %% run_control={"marked": true}
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
sim_dir = "../../../system_log_dir/forecast_evaluator"
prod_dir = "/data/cf_production/CF_2022_08_15/job-sasm_job-jobid-1002348952/user_executable_run_0-1000005033091/cf_prod_system_log_dir"
prod_dir = os.path.join(prod_dir, "process_forecasts/portfolio")

# Simulation data.
print("# sim_dir")
hdbg.dassert_dir_exists(sim_dir)
# !ls {simulation_dir}

# Production data.
print("# prod_dir")
hdbg.dassert_dir_exists(prod_dir)
# !ls {prod_dir}

# %%
date = "2022-08-15"
start_timestamp = pd.Timestamp(date + " 09:30:00", tz="America/New_York")
end_timestamp = pd.Timestamp(date + " 16:00:00", tz="America/New_York")

# %%
hdbg.dassert_dir_exists(root_dir)
dict_ = {
    "portfolio_data_dir": prod_dir,
    "research_data_dir": sim_dir,
    "freq": "15T",
    "portfolio_file_name": None,
    "research_file_name": None,
    "start_timestamp": start_timestamp,
    "end_timestamp": end_timestamp,
}
hdbg.dassert_dir_exists(dict_["portfolio_data_dir"])
hdbg.dassert_dir_exists(dict_["research_data_dir"])

# %% [markdown]
# # Load Portfolio data

# %%
config = cconfig.Config.from_dict(dict_)
#
start_timestamp = config["start_timestamp"]
end_timestamp = config["end_timestamp"]

# Load and time-localize Portfolio logged data.
paper_df, paper_stats_df = oms.Portfolio.read_state(
    config["portfolio_data_dir"],
    #file_name=config["portfolio_file_name"],
)
paper_df = paper_df.loc[start_timestamp:end_timestamp]
display(paper_df.head(3))

paper_stats_df = paper_stats_df.loc[start_timestamp:end_timestamp]
display(paper_stats_df.head(3))

# %% [markdown]
# # Load ForecastEvaluator data

# %%
print(config["research_data_dir"])
# !ls {config["research_data_dir"]}

# %%
# Load and time localize ForecastEvaluator logged data.
# (
#     research_df,
#     research_stats_df,
# ) = dtfmod.ForecastEvaluatorFromReturns.read_portfolio(
#     config["research_data_dir"],
#     file_name=config["research_file_name"],
# )

(
    research_df,
    research_stats_df,
) = dtfmod.ForecastEvaluatorFromPrices.read_portfolio(
    config["research_data_dir"],
    #file_name=config["research_file_name"],
)


# %%
# Load and time-localize Portfolio logged data.
paper_df, paper_stats_df = oms.Portfolio.read_state(
    config["portfolio_data_dir"],
    #file_name=config["portfolio_file_name"],
)

# %%
paper_df = paper_df.loc[start_timestamp:end_timestamp]
paper_stats_df = paper_stats_df.loc[start_timestamp:end_timestamp]


# %%

# %%
print(research_df.columns.levels[0])

#research_df["price"]
#research

# %%
research_df["position"]

# %%
#research_df

# %%
research_stats_df


# %%
def compute_delay(df: pd.DataFrame, freq: str) -> pd.Series:
    diff = df.index - df.index.round(freq)
    srs = pd.Series(
        [
            diff.mean(),
            diff.std(),
        ],
        [
            "mean",
            "stdev",
        ],
        name="delay",
    )
    return srs


# Compute delay stats.
delay_stats = compute_delay(paper_stats_df, config["freq"])
display(delay_stats)

# Round paper_stats_df to bar
paper_stats_df.index = paper_stats_df.index.round(config["freq"])

# %%
bar_stats_df = pd.concat(
    [research_stats_df, paper_stats_df], axis=1, keys=["research", "paper"]
)
display(bar_stats_df.tail(100))

# %%
stats_computer = dtfmod.StatsComputer()
stats_sxs, _ = stats_computer.compute_portfolio_stats(
    bar_stats_df, config["freq"]
)
display(stats_sxs)


# %%
def per_asset_pnl_corr(
    research_df: pd.DataFrame, paper_df: pd.DataFrame, freq: str
) -> pd.Series:
    research_pnl = research_df["pnl"]
    paper_pnl = paper_df["pnl"]
    corrs = {}
    for asset_id in research_pnl.columns:
        pnl1 = research_pnl[asset_id].resample(freq).sum(min_count=1)
        pnl2 = paper_pnl[asset_id].resample(freq).sum(min_count=1)
        corr = pnl1.corr(pnl2)
        corrs[asset_id] = corr
    corr_srs = pd.Series(corrs).rename("pnl_correlation")
    return corr_srs


# %%
# Display per-asset PnL correlations.
pnl_corrs = per_asset_pnl_corr(research_df, paper_df, config["freq"])
pnl_corrs.hist(bins=101)

# %%
pnl_corrs.sort_values().head()

# %%
pnl = bar_stats_df.T.xs("pnl", level=1).T
display(pnl.head())

# %%
#pnl.corr()
pnl[2:].corr()

# %%
coplotti.plot_portfolio_stats(bar_stats_df[2:])
#coplotti.plot_portfolio_stats(bar_stats_df)