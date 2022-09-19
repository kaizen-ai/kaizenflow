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

# %% [markdown]
# # Configure dirs

# %%
sim_dir = "../../../system_log_dir/forecast_evaluator"
#prod_dir = "/data/cf_production/CF_2022_08_15/job-sasm_job-jobid-1002348952/user_executable_run_0-1000005033091/cf_prod_system_log_dir"
prod_dir = "../../../system_log_dir.prod"
prod_dir = os.path.join(prod_dir, "process_forecasts/portfolio")

# Simulation data.
print("# sim_dir")
hdbg.dassert_dir_exists(sim_dir)
# !ls {sim_dir}

# Production data.
print("# prod_dir")
hdbg.dassert_dir_exists(prod_dir)
# !ls {prod_dir}

# %%
date = "2022-08-29"
start_timestamp = pd.Timestamp(date + " 09:30:00", tz="America/New_York")
end_timestamp = pd.Timestamp(date + " 20:00:00", tz="America/New_York")

# %%
dict_ = {
    "portfolio_data_dir": prod_dir,
    "research_data_dir": sim_dir,
    #"freq": "15T",
    "freq": "5T",
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
import oms.process_forecasts_ as oprofore
log_dir = "/app/system_log_dir.prod/process_forecasts"
paper_target_positions_df = oprofore.ForecastProcessor.read_logged_target_positions(log_dir)
paper_target_positions_df.head(3)

# %%
paper_df.head(3)

# %%
paper_stats_df.head(3)

# %% [markdown]
# # Align data

# %%
print(start_timestamp, end_timestamp)
paper_df = paper_df.loc[start_timestamp:end_timestamp]
paper_stats_df = paper_stats_df.loc[start_timestamp:end_timestamp]


# %%
paper_df.head(3)

# %%
research_df.head(3)

# %% [markdown]
# ## Universe comparison

# %% [markdown]
# ## Compare forecasts

# %%
#print(paper_df.columns.levels[0])
print(paper_target_positions_df.columns.levels[0])
print(research_df.columns.levels[0])

#research_df["price"]
#research

# %%
paper_target_positions_df["prediction"].dropna()

# %%
research_df["prediction"].dropna()

# %%
display(paper_target_positions_df["prediction"].head(3))
display(research_df["prediction"].head(3))

# %%
#asset_id = 1467591036
asset_id = 1467591036

# %%
#print(paper_target_positions_df["prediction"][asset_id].index)
#print(research_df["prediction"][asset_id].index)

# %%
#research_df["prediction"][asset_id].index = research_df["prediction"][asset_id].index.snap("5T")
#paper_target_positions_df["prediction"][asset_id].index = 
paper_target_positions_df.index = [dt.round("5T") for dt in paper_target_positions_df["prediction"][asset_id].index]

# %%
paper_target_positions_df["prediction"][asset_id].dropna()

# %%
#asset_id = 1467591036
asset_id = 1467591036
prediction_df = pd.merge(paper_target_positions_df["prediction"][asset_id],
                         research_df["prediction"][asset_id],
                         left_index=True, right_index=True, how="outer")

# prediction_df
prediction_df.dropna()

#display(prediction_df.dropna())

# %%
research_df["prediction"]

# %% [markdown]
# ## Prices

# %%
research_df.columns.levels[0]

# %%
paper_target_positions_df.columns.levels[0]

# %%
target_col = "price"
asset_id = 1467591036
prediction_df = pd.merge(paper_target_positions_df[target_col][asset_id],
                         research_df[target_col][asset_id],
                         left_index=True, right_index=True, how="outer")

# prediction_df
prediction_df["2022-08-29"].plot()

# %%
prediction_df["2022-08-29"].pct_change().dropna().plot()

# %%
prediction_df[:"2022-08-29 20:20:00-04:00"].pct_change().dropna().corr()

# %%
research_df["holdings"]

# %% [markdown]
# ## Holdings

# %% run_control={"marked": false}
ts1 = "2022-08-29 15:20:00-04:00"
ts2 = "2022-08-29 20:20:00-04:00"

# %%
target_col1 = "holdings"
target_col2 = "curr_num_shares"
asset_id = 1467591036
prediction_df = pd.merge(paper_target_positions_df[target_col2][asset_id],
                         research_df[target_col1][asset_id],
                         left_index=True, right_index=True, how="outer")

# prediction_df
df = prediction_df[ts1:ts2]
df /= prediction_df.dropna().iloc[-1]
#df["1467591036_x"] *= -1
df.plot()

# %%
df = prediction_df[ts1:ts2]
df["1467591036_x"] *= -1
df.plot()


# %% [markdown]
# ## Pnl

# %%
#research_df

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
#research_stats_df
paper_stats_df

# %%
bar_stats_df = pd.concat(
    [research_stats_df, paper_stats_df], axis=1, keys=["research", "paper"]
)
#display(bar_stats_df.head(3))
display(bar_stats_df.dropna().tail(3))

# %%
df = bar_stats_df.dropna()[[("research", "pnl"), ("paper", "pnl")]]#.plot()
df["paper", "pnl"] *= 20
df.plot()

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
