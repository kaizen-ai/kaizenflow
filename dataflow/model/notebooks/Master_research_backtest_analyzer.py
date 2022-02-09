# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.3
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

import logging

import core.config as cconfig
import core.plotting as coplotti
import dataflow.model as dtfmod
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hparquet as hparque
import helpers.hprint as hprint

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Load tiled backtest

# %%
dict_ = {
    "file_name": "",
    "asset_id_col": "",
    "prediction_col": "",
    "returns_col": "",
    "volatility_col": "",
    "target_gmv": 1e6,
    "dollar_neutrality": "no_constraint",
    "freq": "5T",
    # "start_timestamp": "",
    # "end_timestamp": "",
}

# %%
config = cconfig.get_config_from_nested_dict(dict_)

# %%
columns = [
    config["asset_id_col"],
    config["prediction_col"],
    config["returns_col"],
    config["volatility_col"],
]

# %%
parquet_df = hparque.from_parquet(config["file_name"], columns=columns)

# %%
df = dtfmod.process_parquet_read_df(parquet_df, config["asset_id_col"])

# %%
_LOG.info("num_assets=%d", len(df.columns.levels[1]))

# %% [markdown]
# # Computer per-bar portfolio stats

# %%
forecast_evaluator = dtfmod.ForecastEvaluator(
    returns_col=config["returns_col"],
    volatility_col=config["volatility_col"],
    prediction_col=config["prediction_col"],
)

# %%
portfolio_df, bar_stats_df = forecast_evaluator.annotate_forecasts(
    df,
    target_gmv=config["target_gmv"],
    dollar_neutrality=config["dollar_neutrality"],
)

# %% [markdown]
# # Computer aggregate portfolio stats

# %%
stats_computer = dtfmod.StatsComputer()

# %%
stats, stats_df = stats_computer.compute_portfolio_stats(
    bar_stats_df, config["freq"]
)
display(stats)

# %% [markdown]
# # Plot portfolio metrics

# %%
coplotti.plot_portfolio_stats(bar_stats_df)
