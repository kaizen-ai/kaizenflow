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

import datetime
import logging

import numpy as np
import pandas as pd

import core.config as cconfig
import core.finance as cofinanc
import core.plotting as coplotti
import core.statistics as costatis
import dataflow.model as dtfmod
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hparquet as hparque
import helpers.hprint as hprint
import helpers.hsql as hsql

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
    "start_date": datetime.date(2010, 1, 1),
    "end_date": datetime.date(2020, 12, 31),
    "asset_id_col": "",
    "returns_col": "",
    "volatility_col": "",
    "prediction_col": "",
    "feature_cols": None,
    "feature_lag": 2,
    "target_col": "",
    "target_gmv": 1e6,
    "dollar_neutrality": "no_constraint",
    "freq": "5T",
}
config = cconfig.get_config_from_nested_dict(dict_)

# %% [markdown]
# ## Report tile stats

# %%
parquet_tile_analyzer = dtfmod.ParquetTileAnalyzer()
parquet_tile_metadata = parquet_tile_analyzer.collate_parquet_tile_metadata(
    config["file_name"]
)

# %%
parquet_tile_analyzer.compute_metadata_stats_by_asset_id(parquet_tile_metadata)

# %%
parquet_tile_analyzer.compute_universe_size_by_time(parquet_tile_metadata)

# %%
asset_ids = parquet_tile_metadata.index.levels[0].to_list()

# %% [markdown]
# ## Load a single-asset tile

# %%
single_asset_tile = next(
    hparque.yield_parquet_tiles_by_assets(
        config["file_name"],
        asset_ids[0:1],
        config["asset_id_col"],
        1,
        None,
    )
)

# %%
single_tile_df = dtfmod.process_parquet_read_df(df, config["asset_id_col"])

# %%
single_tile_df.columns

# %%
single_tile_df.head(3)

# %% [markdown]
# # Overnight returns

# %%
host = ""
dbname = ""
port = 1000
user = ""
password = ""
table_name = ""
connection = hsql.get_connection(host, dbname, port, user, password)

# %%
query_results = cofinanc.query_by_assets_and_dates(
    connection,
    table_name,
    asset_ids=asset_ids,
    asset_id_col=config["asset_id_col"],
    start_date=config["start_date"],
    end_date=config["end_date"],
    date_col="date",
    select_cols=["date", "open_", "close", "total_return", "prev_total_return"],
)

# %%
overnight_returns = cofinanc.compute_overnight_returns(
    query_results,
    config["asset_id_col"],
)

# %% [markdown]
# # Compute portfolio bar metrics

# %%
bar_metrics = dtfmod.generate_bar_metrics(
    config["file_name"],
    config["start_date"],
    config["end_date"],
    config["asset_id_col"],
    config["returns_col"],
    config["volatility_col"],
    config["prediction_col"],
    config["target_gmv"],
    config["dollar_neutrality"],
    # overnight_returns["overnight_returns"],
)

# %%
coplotti.plot_portfolio_stats(bar_metrics, freq="B")

# %% [markdown]
# # Compute aggregate portfolio stats

# %%
stats_computer = dtfmod.StatsComputer()

# %%
portfolio_stats, daily_metrics = stats_computer.compute_portfolio_stats(
    bar_metrics,
    "B",
)
display(portfolio_stats)

# %%
portfolio_stats_at_freq, _ = stats_computer.compute_portfolio_stats(
    bar_metrics,
    config["freq"],
)
display(portfolio_stats_at_freq)

# %% [markdown]
# # Regression analysis

# %%
hdbg.dassert(config["target_col"])
hdbg.dassert(config["feature_cols"])

# %%
coefficients = dtfmod.regress(
    config["file_name"],
    config["asset_id_col"],
    config["target_col"],
    config["feature_cols"],
    config["feature_lag"],
    50,
)

# %%
coefficients.head(3)

# %% [markdown]
# # Predictor mixing

# %%
hdbg.dassert(config["feature_cols"])

# %%
features = config["feature_cols"]
weights = pd.DataFrame(np.identity(len(features)), features, features)
weights["sum"] = 1
display(weights)

# %%
mix_bar_metrics = dtfmod.load_mix_evaluate(
    config["file_name"],
    config["start_date"],
    config["end_date"],
    config["asset_id_col"],
    config["returns_col"],
    config["volatility_col"],
    config["feature_cols"],
    weights,
    config["target_gmv"],
    config["dollar_neutrality"],
)

# %%
mix_portfolio_stats, mix_daily_metrics = stats_computer.compute_portfolio_stats(
    mix_bar_metrics,
    "B",
)
display(mix_portfolio_stats)

# %%
coplotti.plot_portfolio_stats(mix_bar_metrics, freq="B")
